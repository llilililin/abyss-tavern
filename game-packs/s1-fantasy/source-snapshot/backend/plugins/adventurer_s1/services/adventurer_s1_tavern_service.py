"""深渊酒馆 S1 酒馆/社交「只读层」业务逻辑。

职责（最小只读层，不含 AI 探险引擎 / 商店 / 管理工具）：
- 公告：读取 SystemConfig 中的 S1 公告文本
- 聊天：分页读取 + 发言（带字数与冷却限制）
- 探险日志：动态流列表（轻量，不含 full_log）+ 单条详情
- 全体冒险者浏览：分页 / 状态筛选 / 名称搜索 / 排序
- 小队：分页列表（含成员概要）
- 关注 / 取关冒险者

数据访问沿用 S1 既有风格：读用 ``Model.query``，写走 ``db_manager.transaction()``；
批量场景统一预取（item_dict / followed_ids），并对重数据关联走 ``mode='list'``
避免 N+1 与暴露详情专属重字段。所有方法返回 ``(result, error)`` 二元组。
"""
from runtime_core.time import utc_now as _portable_utc_now
from plugins.adventurer_s1.services.s1_player_error_service import player_operation_failure
from datetime import datetime
from typing import Optional, Tuple
from flask import current_app
from sqlalchemy import and_, case, exists, func, or_
from sqlalchemy.orm import defer, joinedload, noload, selectinload
from db_repository import db_manager
from db_repository.decorators import monitor_performance
from utils.db_operation_tracker import track_db_phase
from db_repository.s1_expedition_comment_repository import count_comments_for_target, create_comment, get_comment, get_comment_counts, get_latest_comment_for_user, list_comments, lock_comment_author
from utils.cache_manager import CacheManager
from utils.pagination import build_paginated_response
from extensions import db
from plugins.adventurer_s1.models import AdventurerS1, AdventurerS1NoticeAck, AdventurerS1Party, AdventurerS1ExpeditionLog, AdventurerS1ExpeditionLogMember, AdventurerS1ExpeditionLike, AdventurerS1ExpeditionFavorite, AdventurerS1ChatMessage, AdventurerS1Follow, AdventurerS1PartyMember
from plugins.adventurer_s1.services.adventurer_s1_board_version_service import bump_board_version, get_board_version
from utils.time_utils import to_utc_iso
_NOTICE_CONFIG_KEY = 'adventurer_s1_tavern_notice'
_EXPEDITION_COMMENT_TARGET_TYPES = {'event', 'member_update'}

def _event_has_visible_text(event) -> bool:
    if isinstance(event, str):
        return bool(event.strip())
    if not isinstance(event, dict):
        return False
    for key in ('content', 'text', 'description'):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return True
    return False

def _is_commentable_log_target(full_log: dict, log_status: str, target_type: str, target_index: int) -> bool:
    if target_type not in _EXPEDITION_COMMENT_TARGET_TYPES or target_index < 0:
        return False
    if target_type == 'event':
        events = full_log.get('events') if isinstance(full_log, dict) else None
        return bool(isinstance(events, list) and target_index < len(events) and _event_has_visible_text(events[target_index]))
    if log_status != 'completed':
        return False
    updates = full_log.get('member_updates') if isinstance(full_log, dict) else None
    return bool(isinstance(updates, list) and target_index < len(updates) and isinstance(updates[target_index], dict))

def _commentable_public_full_log(log: AdventurerS1ExpeditionLog) -> dict:
    """Build the same player-visible target container used by the reader."""
    raw = log.full_log if isinstance(log.full_log, dict) else {}
    if log.status in ('generating', 'in_progress'):
        from plugins.adventurer_s1.services.adventurer_s1_log_service import build_in_flight_public_full_log
        return build_in_flight_public_full_log(raw, log_status=log.status)
    if log.status in ('completed', 'failed'):
        from plugins.adventurer_s1.services.adventurer_s1_log_service import build_player_s1_full_log
        return build_player_s1_full_log(raw)
    return {}

def _build_visible_comment_counts(full_log: dict, log_status: str, stored_counts: dict[tuple[str, int], int]) -> dict[str, dict[str, int]]:
    """Return zero-filled counts only for targets visible in the player reader."""
    result: dict[str, dict[str, int]] = {'event': {}, 'member_update': {}}
    events = full_log.get('events') if isinstance(full_log, dict) else None
    if isinstance(events, list):
        for index, event in enumerate(events):
            if _event_has_visible_text(event):
                result['event'][str(index)] = stored_counts.get(('event', index), 0)
    if log_status == 'completed':
        updates = full_log.get('member_updates') if isinstance(full_log, dict) else None
        if isinstance(updates, list):
            for index, update in enumerate(updates):
                if isinstance(update, dict):
                    result['member_update'][str(index)] = stored_counts.get(('member_update', index), 0)
    return result

def _filter_logs_by_adventurer_id(query, adventurer_id: int):
    """按冒险者过滤探险日志：走结构化参与者索引，避免扫描 full_log JSON。"""
    adv_id = int(adventurer_id)
    return query.join(AdventurerS1ExpeditionLogMember, AdventurerS1ExpeditionLogMember.expedition_log_id == AdventurerS1ExpeditionLog.id).filter(AdventurerS1ExpeditionLogMember.adventurer_id == adv_id)
_SORT_COLUMNS = {'level': AdventurerS1.level, 'name': AdventurerS1.name, 'strength': AdventurerS1.strength, 'agility': AdventurerS1.agility, 'intelligence': AdventurerS1.intelligence, 'luck': AdventurerS1.luck, 'gold': AdventurerS1.gold, 'created_at': AdventurerS1.created_at}
_VALID_ADV_STATUS = {'idle', 'in_party', 'exploring', 'resting', 'dead', 'archived'}
_parties_board_cache = CacheManager(ttl_seconds=60, max_size=2000)
_tavern_upgrade_cache = CacheManager(ttl_seconds=60, max_size=1)

def invalidate_parties_board_cache() -> None:
    """小队广场数据变更后清空 board 缓存（全用户共享列表）。"""
    _parties_board_cache.clear()
    bump_board_version('parties')

def invalidate_tavern_upgrade_cache() -> None:
    _tavern_upgrade_cache.clear()


def _backfill_member_update_names(full_log: object) -> None:
    """给 full_log.member_updates 补齐缺失的 name（就地修改）。

    生成阶段写入的 member_updates 只带 id，没有 name。前端成员卡直接读
    ``memberUpdates[].name``，读不到就回退成「冒险者 #id」。玩家视图在
    ``build_player_s1_full_log`` 里会补名，但管理员视图走 ``to_dict`` 原始
    数据，因此在这里统一兜底。
    """
    if not isinstance(full_log, dict):
        return
    updates = full_log.get('member_updates')
    if not isinstance(updates, list):
        return
    missing = []
    for update in updates:
        if not isinstance(update, dict) or update.get('name'):
            continue
        try:
            adv_id = int(update.get('id'))
        except (TypeError, ValueError):
            continue
        missing.append(adv_id)
    if not missing:
        return
    rows = (
        db.session.query(AdventurerS1.id, AdventurerS1.name)
        .filter(AdventurerS1.id.in_(missing))
        .all()
    )
    name_by_id = {row[0]: row[1] for row in rows if row[1]}
    if not name_by_id:
        return
    for update in updates:
        if not isinstance(update, dict) or update.get('name'):
            continue
        try:
            adv_id = int(update.get('id'))
        except (TypeError, ValueError):
            continue
        name = name_by_id.get(adv_id)
        if name:
            update['name'] = name


class AdventurerS1TavernService:

    @staticmethod
    def _get_notice_config():
        from models.system_config import SystemConfig
        return SystemConfig.query.filter_by(config_key=_NOTICE_CONFIG_KEY).first()

    @staticmethod
    def _notice_version(cfg) -> Optional[str]:
        if not cfg or not cfg.updated_at:
            return None
        return to_utc_iso(cfg.updated_at)

    @staticmethod
    def get_notice(user_id: int | None) -> Tuple[dict, Optional[str]]:
        """读取公告；管理员传 None 时不关联任何玩家已读回执。"""
        from models.system_config import SystemConfig
        if user_id is None:
            cfg = SystemConfig.query.filter_by(config_key=_NOTICE_CONFIG_KEY).first()
            ack = None
        else:
            row = db.session.query(SystemConfig, AdventurerS1NoticeAck).outerjoin(AdventurerS1NoticeAck, AdventurerS1NoticeAck.user_id == int(user_id)).filter(SystemConfig.config_key == _NOTICE_CONFIG_KEY).first()
            cfg, ack = row if row else (None, None)
        if cfg is None:
            return ({'notice': '', 'version': None, 'should_show': False}, None)
        notice = cfg.config_value.strip() if cfg.config_value else ''
        version = AdventurerS1TavernService._notice_version(cfg) if notice else None
        should_show = bool(notice and version and (not ack or ack.notice_version != version))
        return ({'notice': notice, 'version': version, 'should_show': should_show}, None)

    @staticmethod
    def ack_notice(user_id: int, version: str) -> Tuple[Optional[dict], Optional[str]]:
        """玩家确认已读当前公告；版本不匹配则拒绝（防止确认过期公告）。"""
        cfg = AdventurerS1TavernService._get_notice_config()
        notice = cfg.config_value.strip() if cfg and cfg.config_value else ''
        current_version = AdventurerS1TavernService._notice_version(cfg)
        if not notice or not current_version:
            return (None, '当前没有可确认的公告')
        if version != current_version:
            return (None, '公告已更新，请刷新后重新确认')
        ack = AdventurerS1NoticeAck.query.filter_by(user_id=user_id).first()
        try:
            with db_manager.transaction():
                if ack:
                    ack.notice_version = current_version
                else:
                    db_manager.add(AdventurerS1NoticeAck(user_id=user_id, notice_version=current_version))
        except Exception as e:
            return (None, player_operation_failure(e, '确认失败'))
        return ({'version': current_version, 'should_show': False}, None)

    @staticmethod
    def admin_set_notice(notice: str) -> Tuple[Optional[dict], Optional[str]]:
        """管理员设置或更新酒馆公告（更新后所有玩家需重新确认）。"""
        from models.system_config import SystemConfig
        notice = (notice or '').strip()
        if len(notice) > 2000:
            return (None, '公告内容不能超过 2000 字')
        cfg = AdventurerS1TavernService._get_notice_config()
        now = _portable_utc_now()
        try:
            with db_manager.transaction():
                if cfg:
                    cfg.config_value = notice
                    cfg.updated_at = now
                elif notice:
                    db_manager.add(SystemConfig(config_key=_NOTICE_CONFIG_KEY, config_value=notice, description='深渊酒馆 S1 顶部公告', updated_at=now))
                else:
                    return ({'notice': '', 'version': None, 'should_show': False}, None)
        except Exception as e:
            return (None, f'保存公告失败：{e}')
        version = to_utc_iso(now) if notice else None
        return ({'notice': notice, 'version': version, 'should_show': bool(notice)}, None)

    @staticmethod
    @monitor_performance('s1.tavern.list_adventurers')
    def list_adventurers(page: int, per_page: int, status: Optional[str]=None, search: Optional[str]=None, sort_field: Optional[str]=None, sort_order: Optional[str]=None, current_user_id: Optional[int]=None) -> Tuple[Optional[dict], Optional[str]]:
        """分页浏览全体冒险者（非仅自己）。支持状态筛选 / 名称搜索 / 排序。

        登录用户下，自己拥有的冒险者与已关注的冒险者固定排在最前，
        再按 sort_field / sort_order 做二级排序。
        """
        from plugins.adventurer_s1.services.adventurer_s1_service import AdventurerS1Service
        from db_repository.s1_tavern_read_repository import tavern_card_read_session
        from plugins.adventurer_s1.services.s1_squad_service import S1SquadService
        with tavern_card_read_session() as session:
            q = session.query(AdventurerS1).options(*AdventurerS1Service._adventurer_card_query_options())
            if status and status in _VALID_ADV_STATUS:
                q = q.filter(AdventurerS1.status == status)
            elif not status:
                q = q.filter(AdventurerS1.status.notin_(AdventurerS1.TAVERN_EXCLUDED_STATUSES))
            if search and search.strip():
                q = q.filter(AdventurerS1.name.ilike(f'%{search.strip()}%'))
            with track_db_phase('count'):
                total = q.with_entities(func.count(AdventurerS1.id)).scalar() or 0
            order_clauses = []
            followed_expr = None
            if current_user_id:
                followed_expr = exists().where((AdventurerS1Follow.user_id == current_user_id) & (AdventurerS1Follow.adventurer_id == AdventurerS1.id))
                priority_conds = [(AdventurerS1.owner_id == current_user_id) & AdventurerS1.is_player_created.is_(True), followed_expr]
                order_clauses.append(case((or_(*priority_conds), 0), else_=1))
            col = _SORT_COLUMNS.get(sort_field or 'created_at', AdventurerS1.created_at)
            order_clauses.append(col.asc() if (sort_order or 'desc').lower() == 'asc' else col.desc())
            order_clauses.append(AdventurerS1.id.desc())
            q = q.order_by(*order_clauses)
            with track_db_phase('page_fetch'):
                if followed_expr is not None:
                    page_rows = q.add_columns(followed_expr.label('is_followed')).offset((page - 1) * per_page).limit(per_page).all()
                    rows = [row[0] for row in page_rows]
                    followed_id_set = {row[0].id for row in page_rows if bool(row[1])}
                else:
                    rows = q.offset((page - 1) * per_page).limit(per_page).all()
                    followed_id_set = set()
            with track_db_phase('card_metadata'):
                from plugins.adventurer_s1.services.adventurer_s1_tavern_seat_service import AdventurerS1TavernSeatService
                seated_ids = AdventurerS1TavernSeatService.get_seated_adventurer_ids([adv.id for adv in rows], session=session)
                building_info_map = AdventurerS1Service.build_adventurer_building_info_map(rows, session=session)
                dorm_recovery_paused_ids = AdventurerS1Service._dorm_recovery_paused_adventurer_ids(rows, session=session)
                clue_meta = AdventurerS1Service._batch_clue_meta_by_item_names({equipment.name for adv in rows for equipment in adv.equipment_items}, session=session)
                from plugins.adventurer_s1.services.s1_item_use_condition_service import sanitize_adventurer_payload_items
                from plugins.adventurer_s1.services.s1_trait_loadout_service import build_card_trait_loadout_states
                trait_loadout_states = build_card_trait_loadout_states((trait for adv in rows for trait in adv.traits), session=session)
                badge_items = [{'id': adv.id} for adv in rows]
                S1SquadService.attach_badges(badge_items, session=session)
                badges_by_id = {item['id']: item for item in badge_items}
        with track_db_phase('serialize'):
            items = []
            for adv in rows:
                item = adv.to_dict(current_user_id=current_user_id, item_clue_meta=clue_meta, is_followed=adv.id in followed_id_set, mode='list', trait_loadout_states=trait_loadout_states, tavern_seated=adv.id in seated_ids, building_info=building_info_map.get(adv.id))
                item['dorm_recovery_paused'] = adv.id in dorm_recovery_paused_ids
                sanitize_adventurer_payload_items(item)
                if 'squad_badge' in badges_by_id[adv.id]:
                    item['squad_badge'] = badges_by_id[adv.id]['squad_badge']
                items.append(item)
        return (build_paginated_response(items, total, page, per_page), None)

    @staticmethod
    def _apply_party_status_filter(q, status: Optional[str]):
        if status == 'active':
            return q.filter(AdventurerS1Party.status.in_(['forming', 'exploring']))
        if status and status != 'all':
            return q.filter(AdventurerS1Party.status == status)
        return q

    @staticmethod
    def _user_party_ids_subquery(user_id: int):
        return db.session.query(AdventurerS1PartyMember.party_id).join(AdventurerS1, AdventurerS1.id == AdventurerS1PartyMember.adventurer_id).filter(AdventurerS1.owner_id == user_id).distinct().subquery()

    @staticmethod
    def _get_user_party_ids(user_id: int) -> list[int]:
        rows = db.session.query(AdventurerS1PartyMember.party_id).join(AdventurerS1, AdventurerS1.id == AdventurerS1PartyMember.adventurer_id).filter(AdventurerS1.owner_id == user_id).distinct().all()
        return [party_id for party_id, in rows]

    @staticmethod
    def _party_list_member_load():
        """小队列表成员卡片只需主表标量字段，跳过 traits/equipment 等 selectin。"""
        return selectinload(AdventurerS1Party.members).options(selectinload(AdventurerS1.active_title_config), noload(AdventurerS1.traits), noload(AdventurerS1.equipment_items), noload(AdventurerS1.skill_items), noload(AdventurerS1.investor_items), noload(AdventurerS1.dorm_room), noload(AdventurerS1.story_threads), defer(AdventurerS1.history_story), defer(AdventurerS1.expedition_buff), defer(AdventurerS1.building_usage_extra), defer(AdventurerS1.closure_resolution))

    @staticmethod
    def _fetch_parties_rows(q, page: int, per_page: int):
        return q.options(AdventurerS1TavernService._party_list_member_load()).order_by(AdventurerS1Party.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()

    @staticmethod
    def _fetch_parties_page(q, page: int, per_page: int):
        q = q.options(AdventurerS1TavernService._party_list_member_load())
        total = q.order_by(None).count()
        rows = q.order_by(AdventurerS1Party.created_at.desc()).offset((page - 1) * per_page).limit(per_page).all()
        return (rows, total)

    @staticmethod
    def _build_party_items(rows, *, include_departure_votes: bool=False) -> list[dict]:
        from plugins.adventurer_s1.services.adventurer_s1_party_service import get_departure_votes_by_party_ids
        votes_map = {}
        if include_departure_votes and rows:
            votes_map = get_departure_votes_by_party_ids([p.id for p in rows])
        return [p.to_dict(departure_voted_adventurer_ids=votes_map.get(p.id, []) if include_departure_votes else None) for p in rows]

    @staticmethod
    def _special_dungeon_names() -> list[str]:
        from plugins.adventurer_s1.services.adventurer_s1_config_service import S1_SPECIAL_DUNGEONS
        return [d['name'] for d in S1_SPECIAL_DUNGEONS]

    @staticmethod
    def _matched_party_ids_subquery():
        from plugins.adventurer_s1.models import AdventurerS1MatchTicket
        return db.session.query(AdventurerS1MatchTicket.matched_party_id).filter(AdventurerS1MatchTicket.matched_party_id.isnot(None))

    @staticmethod
    def _apply_s1_party_ui_filter(q, *, browse: bool=False):
        """旧版普通浏览/手动组队已下线：浏览仅特殊机缘；我的小队另含已出发探险中队伍。"""
        special_names = AdventurerS1TavernService._special_dungeon_names()
        if browse:
            return q.filter(AdventurerS1Party.target_dungeon.in_(special_names))
        return q.filter(or_(AdventurerS1Party.target_dungeon.in_(special_names), AdventurerS1Party.status == 'exploring'))

    @staticmethod
    def list_parties(page: int, per_page: int, status: Optional[str]='active', current_user_id: Optional[int]=None, mine: bool=False, exclude_mine: bool=False) -> Tuple[Optional[dict], Optional[str]]:
        """分页浏览小队。status=active 表示集结中+探险中；其余按精确状态过滤。

        mine=1：仅返回当前用户冒险者所在的小队（「我的小队」区块）。
        exclude_mine=1：排除当前用户所在小队（浏览列表去重）。
        """
        q = AdventurerS1TavernService._apply_party_status_filter(AdventurerS1Party.query, status)
        if not mine:
            q = AdventurerS1TavernService._apply_s1_party_ui_filter(q, browse=True)
        if current_user_id and (mine or exclude_mine):
            my_party_ids_sq = AdventurerS1TavernService._user_party_ids_subquery(current_user_id)
            if mine:
                q = q.filter(AdventurerS1Party.id.in_(my_party_ids_sq.select()))
            elif exclude_mine:
                q = q.filter(~AdventurerS1Party.id.in_(my_party_ids_sq.select()))
        rows, total = AdventurerS1TavernService._fetch_parties_page(q, page, per_page)
        items = AdventurerS1TavernService._build_party_items(rows, include_departure_votes=mine)
        return (build_paginated_response(items, total, page, per_page), None)

    @staticmethod
    def list_parties_board(current_user_id: int, status: Optional[str]='active', per_page: int=10, client_version: Optional[str]=None) -> Tuple[Optional[dict], Optional[str]]:
        """合并返回「我的小队」+「浏览小队」，单次调用共享用户小队 ID 与投票批量查询。"""
        version = get_board_version('parties')
        if version is not None and client_version == version:
            return ({'changed': False, 'version': version}, None)
        cache_key = f'parties_board:{current_user_id}:{status}:{per_page}:{version}'
        cached = _parties_board_cache.get(cache_key)
        if cached is not None:
            return ({**cached, 'changed': True, 'version': version}, None)
        my_party_ids = AdventurerS1TavernService._get_user_party_ids(current_user_id)
        base_q = AdventurerS1TavernService._apply_party_status_filter(AdventurerS1Party.query, status)
        if my_party_ids:
            my_q = base_q.filter(AdventurerS1Party.id.in_(my_party_ids))
            browse_q = base_q.filter(~AdventurerS1Party.id.in_(my_party_ids))
            browse_q = AdventurerS1TavernService._apply_s1_party_ui_filter(browse_q, browse=True)
        else:
            my_q = base_q.filter(db.false())
            browse_q = AdventurerS1TavernService._apply_s1_party_ui_filter(base_q, browse=True)
        my_rows = AdventurerS1TavernService._fetch_parties_rows(my_q, 1, per_page)
        browse_rows, browse_total = AdventurerS1TavernService._fetch_parties_page(browse_q, 1, per_page)
        from plugins.adventurer_s1.services.adventurer_s1_party_service import get_departure_votes_by_party_ids
        all_ids = [p.id for p in my_rows] + [p.id for p in browse_rows]
        votes_map = get_departure_votes_by_party_ids(all_ids)
        payload = {'my_parties': [p.to_dict(departure_voted_adventurer_ids=votes_map.get(p.id, [])) for p in my_rows], 'parties': build_paginated_response([p.to_dict() for p in browse_rows], browse_total, 1, per_page)}
        _parties_board_cache.set(cache_key, payload)
        return ({**payload, 'changed': True, 'version': version}, None)

    @staticmethod
    def list_logs(page: int, per_page: int, search: Optional[str]=None, adventurer_id: Optional[int]=None, status: Optional[str]=None, category: Optional[str]=None, user_id: Optional[int]=None, exclude_failed: bool=False) -> Tuple[Optional[dict], Optional[str]]:
        """探险日志动态流（轻量，不含 full_log），支持特殊事件与个人收藏分类。"""
        if category not in (None, '', 'special', 'favorite'):
            return (None, '日志分类无效')
        if category == 'favorite' and (not user_id):
            return (None, '请先登录后查看收藏')
        search_term = (search or '').strip()
        q = db_manager.query(AdventurerS1ExpeditionLog)
        feed_limit = int(current_app.config.get('ADVENTURER_S1_LOG_FEED_LIMIT', 3000))
        if adventurer_id:
            q = _filter_logs_by_adventurer_id(q, adventurer_id)
        if status:
            q = q.filter(AdventurerS1ExpeditionLog.status == status)
        elif exclude_failed:
            q = q.filter(AdventurerS1ExpeditionLog.status != 'failed')
        if search_term:
            kw = f'%{search_term}%'
            q = q.filter(db.or_(AdventurerS1ExpeditionLog.party_name.ilike(kw), and_(AdventurerS1ExpeditionLog.status.in_(('in_progress', 'completed')), or_(AdventurerS1ExpeditionLog.target_dungeon.ilike(kw), AdventurerS1ExpeditionLog.temporary_location_name.ilike(kw))), AdventurerS1ExpeditionLog.result_summary.ilike(kw)))
        if category == 'special':
            q = q.filter(or_(and_(AdventurerS1ExpeditionLog.special_event_theme.isnot(None), AdventurerS1ExpeditionLog.special_event_theme != ''), and_(AdventurerS1ExpeditionLog.special_event_theme.is_(None), AdventurerS1ExpeditionLog.full_log['has_special_event'].as_boolean().is_(True))), AdventurerS1ExpeditionLog.status.in_(('in_progress', 'completed')))
        elif category == 'favorite':
            q = q.filter(exists().where(and_(AdventurerS1ExpeditionFavorite.expedition_log_id == AdventurerS1ExpeditionLog.id, AdventurerS1ExpeditionFavorite.user_id == int(user_id))))
        use_feed_cap = not search_term and (not adventurer_id) and (not status) and (not category)
        skip_exact_count = bool(adventurer_id)
        total: int | None = None
        if use_feed_cap:
            window_subq = q.with_entities(AdventurerS1ExpeditionLog.id).order_by(AdventurerS1ExpeditionLog.created_at.desc()).limit(feed_limit).subquery()
            total = db_manager.query(func.count()).select_from(window_subq).scalar() or 0
            q = db_manager.query(AdventurerS1ExpeditionLog).filter(AdventurerS1ExpeditionLog.id.in_(db_manager.query(window_subq.c.id)))
        elif not skip_exact_count:
            total = q.order_by(None).with_entities(func.count(AdventurerS1ExpeditionLog.id)).scalar() or 0
        fetch_limit = per_page + 1 if skip_exact_count else per_page
        rows = q.options(defer(AdventurerS1ExpeditionLog.full_log)).order_by(AdventurerS1ExpeditionLog.created_at.desc()).offset((page - 1) * per_page).limit(fetch_limit).all()
        if skip_exact_count:
            has_next = len(rows) > per_page
            rows = rows[:per_page]
            total = (page - 1) * per_page + len(rows) + (1 if has_next else 0)
        items = [log.to_summary_dict() for log in rows]
        if rows:
            ids = [log.id for log in rows]
            favorite_ids = set()
            if user_id:
                favorite_ids = {int(row[0]) for row in db_manager.query(AdventurerS1ExpeditionFavorite.expedition_log_id).filter(AdventurerS1ExpeditionFavorite.user_id == int(user_id), AdventurerS1ExpeditionFavorite.expedition_log_id.in_(ids)).all()}
            theme_by_id = {int(log.id): log.special_event_theme for log in rows}
            legacy_ids = [int(log.id) for log in rows if log.special_event_theme is None]
            if legacy_ids:
                legacy_theme_rows = db_manager.query(AdventurerS1ExpeditionLog.id, AdventurerS1ExpeditionLog.full_log['has_special_event'].as_boolean(), AdventurerS1ExpeditionLog.full_log['is_extreme_crisis'].as_boolean(), AdventurerS1ExpeditionLog.full_log['special_event_type'].as_string(), AdventurerS1ExpeditionLog.full_log['special_event_risk_tier'].as_string()).filter(AdventurerS1ExpeditionLog.id.in_(legacy_ids)).all()
                from plugins.adventurer_s1.services.adventurer_s1_config_service import resolve_s1_special_event_theme
                theme_by_id.update({int(row[0]): resolve_s1_special_event_theme({'has_special_event': row[1], 'is_extreme_crisis': row[2], 'special_event_type': row[3], 'special_event_risk_tier': row[4]}) for row in legacy_theme_rows})
            for item in items:
                if item.get('status') in ('in_progress', 'completed'):
                    item['special_event_theme'] = theme_by_id.get(int(item['id']))
                item['is_favorited'] = int(item['id']) in favorite_ids
        return (build_paginated_response(items, total or 0, page, per_page), None)

    @staticmethod
    def get_log_detail(log_id: int, user_id: Optional[int]=None, is_admin: bool=False, reveal_full: bool=False) -> Tuple[Optional[dict], Optional[str]]:
        """单条探险日志详情（含 full_log + 好感度变化摘要）。

        分批推进期间（generating / in_progress）非管理员只能看到已推进的事件；
        completed/failed 非管理员走 ``build_player_s1_full_log`` 白名单（见
        ``adventurer_s1_log_service``）。管理员 ``reveal_full=1`` 可见 in_progress 全量。

        顶层 ``members``（仅名字字符串）供前端名字高亮，不写入裁剪后的 full_log。
        """
        log = AdventurerS1ExpeditionLog.query.session.get(AdventurerS1ExpeditionLog, log_id)
        if not log:
            return (None, '日志不存在')
        raw_full_log = log.full_log if isinstance(log.full_log, dict) else {}
        data = log.to_dict(is_admin=is_admin)
        if not is_admin and log.status not in ('in_progress', 'completed'):
            data['target_dungeon'] = ''
            data['hidden_dungeon'] = None
        from plugins.adventurer_s1.services.adventurer_s1_config_service import resolve_s1_special_event_event_index, resolve_s1_special_event_theme
        if log.status in ('in_progress', 'completed'):
            data['special_event_theme'] = resolve_s1_special_event_theme(raw_full_log)
            data['special_event_event_index'] = resolve_s1_special_event_event_index(raw_full_log)
        full_log = data.get('full_log')
        in_flight = log.status in ('generating', 'in_progress')
        if in_flight and (not (is_admin and reveal_full)):
            from plugins.adventurer_s1.services.adventurer_s1_log_service import build_in_flight_public_full_log
            if not isinstance(full_log, dict):
                data['full_log'] = build_in_flight_public_full_log({}, log_status=log.status)
            else:
                data['full_log'] = build_in_flight_public_full_log(full_log, log_status=log.status)
            data['relationship_changes'] = []
            if is_admin and log.status == 'generating':
                from plugins.adventurer_s1.services.adventurer_s1_expedition_runner import is_s1_generating_stuck
                data['generating_retry_available'] = is_s1_generating_stuck(log)
        else:
            data['relationship_changes'] = AdventurerS1TavernService._compute_log_relationship_changes(log, include_delta=is_admin) if log.status == 'completed' else []
            if not is_admin and log.status in ('completed', 'failed'):
                from plugins.adventurer_s1.services.adventurer_s1_log_service import build_player_s1_full_log
                data['full_log'] = build_player_s1_full_log(full_log if isinstance(full_log, dict) else {})
        from plugins.adventurer_s1.services.adventurer_s1_log_service import resolve_expedition_log_member_names
        data['members'] = resolve_expedition_log_member_names(log, raw_full_log, data.get('full_log'))
        # member_updates 原始数据只带 id，前端读不到 name 会回退成「冒险者 #id」。
        # 统一在此补齐名字（管理员视图走 to_dict 原始数据，不经过裁剪阶段的补名）。
        _backfill_member_update_names(data.get('full_log'))
        visible_full_log = _commentable_public_full_log(log) if is_admin else data.get('full_log') if isinstance(data.get('full_log'), dict) else {}
        data['comment_counts'] = _build_visible_comment_counts(visible_full_log, log.status, get_comment_counts(log.id))
        data['like_info'] = AdventurerS1TavernService._build_log_like_info(log, user_id)
        if user_id and log.status == 'completed':
            from plugins.adventurer_s1.services.s1_ai_feedback_service import S1AiFeedbackService
            ai_feedback = S1AiFeedbackService.get_expedition_target(user_id, log.id)
            if ai_feedback is not None:
                data['ai_feedback'] = ai_feedback
        data['is_favorited'] = bool(user_id and db_manager.query(exists().where(and_(AdventurerS1ExpeditionFavorite.user_id == int(user_id), AdventurerS1ExpeditionFavorite.expedition_log_id == int(log.id)))).scalar())
        if is_admin and isinstance(raw_full_log, dict):
            outline = raw_full_log.get('objective_outline')
            if isinstance(outline, dict) and outline:
                data['objective_outline'] = outline
            objective_rejected_reason = raw_full_log.get('objective_outline_rejected_reason')
            if objective_rejected_reason:
                data['objective_outline_rejected_reason'] = objective_rejected_reason
            objective_raw_excerpt = raw_full_log.get('objective_outline_raw_excerpt')
            if objective_raw_excerpt:
                data['objective_outline_raw_excerpt'] = objective_raw_excerpt
            objective_warnings = raw_full_log.get('objective_outline_validation_warnings')
            if isinstance(objective_warnings, list) and objective_warnings:
                data['objective_outline_validation_warnings'] = objective_warnings
            failure_detail = raw_full_log.get('generation_failure_detail')
            if failure_detail:
                data['generation_failure_detail'] = failure_detail
            retry_trace = raw_full_log.get('generation_retry_trace')
            if isinstance(retry_trace, list) and retry_trace:
                data['generation_retry_trace'] = retry_trace
            for failure_field in ('generation_failure_category', 'generation_failure_code'):
                failure_value = raw_full_log.get(failure_field)
                if failure_value:
                    data[failure_field] = failure_value
        return (data, None)

    @staticmethod
    def set_log_favorite(log_id: int, user_id: int, favorited: bool) -> Tuple[Optional[dict], Optional[str]]:
        """幂等收藏或取消收藏指定探险日志。"""
        with db_manager.transaction():
            log_exists = db_manager.query(exists().where(AdventurerS1ExpeditionLog.id == int(log_id))).scalar()
            if not log_exists:
                return (None, '日志不存在')
            values = {'user_id': int(user_id), 'expedition_log_id': int(log_id)}
            if favorited:
                dialect = db.engine.dialect.name
                if dialect == 'postgresql':
                    from sqlalchemy.dialects.postgresql import insert
                    statement = insert(AdventurerS1ExpeditionFavorite).values(**values)
                    statement = statement.on_conflict_do_nothing(index_elements=['user_id', 'expedition_log_id'])
                    db.session.execute(statement)
                elif dialect == 'sqlite':
                    from sqlalchemy.dialects.sqlite import insert
                    statement = insert(AdventurerS1ExpeditionFavorite).values(**values)
                    statement = statement.on_conflict_do_nothing(index_elements=['user_id', 'expedition_log_id'])
                    db.session.execute(statement)
                else:
                    existing = db_manager.query(AdventurerS1ExpeditionFavorite.id).filter_by(**values).first()
                    if not existing:
                        db_manager.add(AdventurerS1ExpeditionFavorite(**values))
            else:
                db_manager.query(AdventurerS1ExpeditionFavorite).filter_by(**values).delete(synchronize_session=False)
        return ({'log_id': int(log_id), 'is_favorited': bool(favorited)}, None)

    @staticmethod
    def list_expedition_comments(log_id: int, target_type: str, target_index: int, page: int, per_page: int) -> Tuple[Optional[dict], Optional[str]]:
        """分页读取一个玩家可见日志位置的评论。"""
        log = AdventurerS1ExpeditionLog.query.session.get(AdventurerS1ExpeditionLog, log_id)
        if not log:
            return (None, '日志不存在')
        visible_full_log = _commentable_public_full_log(log)
        if not _is_commentable_log_target(visible_full_log, log.status, target_type, target_index):
            return (None, '评论位置不存在或尚未公开')
        items, total = list_comments(log.id, target_type, target_index, page=page, per_page=per_page)
        pages = (total + per_page - 1) // per_page if total else 0
        return ({'items': [item.to_public_dict() for item in items], 'page': page, 'per_page': per_page, 'total': total, 'pages': pages, 'has_next': page < pages, 'has_prev': page > 1}, None)

    @staticmethod
    def post_expedition_comment(log_id: int, user_id: int, target_type: str, target_index: int, content: str) -> Tuple[Optional[dict], Optional[str]]:
        """在一个玩家可见日志位置发表带冷却的评论。"""
        if not isinstance(content, str):
            return (None, '评论内容不能为空')
        content = content.strip()
        if not content:
            return (None, '评论内容不能为空')
        max_length = int(current_app.config.get('ADVENTURER_S1_EXPEDITION_COMMENT_MAX_LENGTH', 300))
        if len(content) > max_length:
            return (None, f'评论内容不能超过 {max_length} 字')
        log = AdventurerS1ExpeditionLog.query.session.get(AdventurerS1ExpeditionLog, log_id)
        if not log:
            return (None, '日志不存在')
        visible_full_log = _commentable_public_full_log(log)
        if not _is_commentable_log_target(visible_full_log, log.status, target_type, target_index):
            return (None, '评论位置不存在或尚未公开')
        cooldown = int(current_app.config.get('ADVENTURER_S1_EXPEDITION_COMMENT_COOLDOWN_SECONDS', 30))
        cooldown = 0
        comment_id = None
        try:
            with db_manager.transaction():
                if not lock_comment_author(user_id):
                    return (None, '用户不存在')
                if cooldown > 0:
                    last = get_latest_comment_for_user(user_id)
                    if last and last.created_at:
                        elapsed = (_portable_utc_now() - last.created_at).total_seconds()
                        if elapsed < cooldown:
                            remaining = int(cooldown - elapsed) + 1
                            return (None, f'评论太频繁，请 {remaining} 秒后再试')
                comment = create_comment(log.id, target_type, target_index, user_id, content)
                comment_id = comment.id
        except Exception:
            current_app.logger.exception('S1 expedition comment creation failed: log_id=%s target=%s:%s', log_id, target_type, target_index)
            return (None, '评论发表失败，请稍后重试')
        saved = get_comment(comment_id)
        if not saved:
            return (None, '评论发表失败，请稍后重试')
        return ({'comment': saved.to_public_dict(), 'comment_count': count_comments_for_target(log.id, target_type, target_index)}, None)

    @staticmethod
    def _latest_completed_log_id_for_adventurer(adventurer_id: int) -> Optional[int]:
        row = db_manager.query(AdventurerS1ExpeditionLog.id).join(AdventurerS1ExpeditionLogMember, AdventurerS1ExpeditionLogMember.expedition_log_id == AdventurerS1ExpeditionLog.id).filter(AdventurerS1ExpeditionLogMember.adventurer_id == int(adventurer_id), AdventurerS1ExpeditionLog.status == 'completed').order_by(AdventurerS1ExpeditionLog.created_at.desc(), AdventurerS1ExpeditionLog.id.desc()).first()
        return int(row[0]) if row else None

    @staticmethod
    def _log_like_member_rows(log_id: int) -> list[dict]:
        rows = db_manager.query(AdventurerS1.id, AdventurerS1.name, AdventurerS1.praise_count, AdventurerS1.owner_id).join(AdventurerS1ExpeditionLogMember, AdventurerS1ExpeditionLogMember.adventurer_id == AdventurerS1.id).filter(AdventurerS1ExpeditionLogMember.expedition_log_id == int(log_id)).order_by(AdventurerS1.id.asc()).all()
        return [{'id': int(row.id), 'name': row.name, 'praise_count': int(row.praise_count or 0), 'owner_id': row.owner_id} for row in rows]

    @staticmethod
    def _build_log_like_info(log: AdventurerS1ExpeditionLog, user_id: Optional[int]) -> dict:
        base = {'can_like': False, 'reason': '', 'from_adventurer_id': None, 'liked_adventurer_id': None, 'liked_adventurer_name': None, 'candidates': []}
        if not user_id:
            base['reason'] = '请先登录后再点赞'
            return base
        if log.status != 'completed':
            base['reason'] = '探险结算完成后才能点赞'
            return base
        members = AdventurerS1TavernService._log_like_member_rows(log.id)
        owned_member_ids = [m['id'] for m in members if m.get('owner_id') == user_id]
        if not owned_member_ids:
            base['reason'] = '只有本次探险成员的玩家可以点赞'
            return base
        existing_like = db_manager.query(AdventurerS1ExpeditionLike).filter(AdventurerS1ExpeditionLike.expedition_log_id == log.id, AdventurerS1ExpeditionLike.from_adventurer_id.in_(owned_member_ids)).first()
        if existing_like:
            liked = next((m for m in members if m['id'] == existing_like.to_adventurer_id), None)
            base.update({'from_adventurer_id': int(existing_like.from_adventurer_id), 'liked_adventurer_id': int(existing_like.to_adventurer_id), 'liked_adventurer_name': liked.get('name') if liked else None, 'reason': '你已经给本次探险的队友点过赞', 'candidates': [{k: m[k] for k in ('id', 'name', 'praise_count')} for m in members if m['id'] != existing_like.from_adventurer_id]})
            return base
        from_adventurer_id = None
        for adv_id in owned_member_ids:
            if AdventurerS1TavernService._latest_completed_log_id_for_adventurer(adv_id) == log.id:
                from_adventurer_id = adv_id
                break
        if not from_adventurer_id:
            base['reason'] = '只能给上一把探险的队友点赞'
            return base
        candidates = [{k: m[k] for k in ('id', 'name', 'praise_count')} for m in members if m['id'] != from_adventurer_id]
        base.update({'can_like': bool(candidates), 'reason': '' if candidates else '没有可点赞的队友', 'from_adventurer_id': from_adventurer_id, 'candidates': candidates})
        return base

    @staticmethod
    def like_expedition_log(log_id: int, user_id: int, to_adventurer_id: int) -> Tuple[Optional[dict], Optional[str]]:
        """当前玩家用自己的本次探险冒险者，给最近一次已完成探险中的一名队友点赞。"""
        log = AdventurerS1ExpeditionLog.query.session.get(AdventurerS1ExpeditionLog, log_id)
        if not log:
            return (None, '日志不存在')
        like_info = AdventurerS1TavernService._build_log_like_info(log, user_id)
        if not like_info.get('can_like'):
            return (None, like_info.get('reason') or '当前不能点赞')
        target_id = int(to_adventurer_id or 0)
        candidate_ids = {int(c['id']) for c in like_info.get('candidates') or []}
        if target_id not in candidate_ids:
            return (None, '只能给本次探险的队友点赞')
        from_adventurer_id = int(like_info['from_adventurer_id'])
        try:
            with db_manager.transaction():
                db_manager.add(AdventurerS1ExpeditionLike(user_id=int(user_id), expedition_log_id=int(log_id), from_adventurer_id=from_adventurer_id, to_adventurer_id=target_id))
                db_manager.query(AdventurerS1).filter(AdventurerS1.id == target_id).update({AdventurerS1.praise_count: AdventurerS1.praise_count + 1}, synchronize_session=False)
        except Exception as e:
            return (None, player_operation_failure(e, '点赞失败'))
        try:
            from plugins.adventurer_s1.services.s1_socket_service import s1_socketio as socketio
            from plugins.adventurer_s1.services.adventurer_s1_service import invalidate_adventurer_detail_cache
            invalidate_adventurer_detail_cache(target_id)
            socketio.emit('adventurer_changed', {'action': 'expedition_like_created', 'ids': [target_id]}, namespace='/')
        except Exception:
            pass
        data, err = AdventurerS1TavernService.get_log_detail(log_id, user_id=user_id)
        if err:
            return ({'message': '点赞成功'}, None)
        return ({'message': '点赞成功', 'log': data}, None)

    @staticmethod
    def _compute_log_relationship_changes(log, include_delta: bool=False) -> list:
        """计算本次探险日志中 AI 事件产生的所有好感度变化，去重合并。

        返回 [{adventurer_a_id, adventurer_a_name, adventurer_b_id, adventurer_b_name,
        direction('up'/'down')}]；具体数值 delta 仅 include_delta=True（管理员）时附带，
        玩家侧不下发任何好感度数值。
        """
        changes = {}
        full_log = log.full_log or {}
        for update in full_log.get('member_updates', []):
            if not isinstance(update, dict):
                continue
            try:
                source_id = int(update.get('id'))
            except (TypeError, ValueError):
                continue
            for rc in update.get('relationship_changes') or []:
                if not isinstance(rc, dict):
                    continue
                try:
                    target_id = int(rc.get('target_id'))
                    delta = int(rc.get('delta', 0))
                except (TypeError, ValueError):
                    continue
                if not source_id or not target_id or delta == 0:
                    continue
                a_id, b_id = (min(source_id, target_id), max(source_id, target_id))
                changes[a_id, b_id] = changes.get((a_id, b_id), 0) + delta
        if not changes:
            return []
        member_ids = {adventurer_id for pair in changes for adventurer_id in pair}
        adv_names = dict(AdventurerS1.query.with_entities(AdventurerS1.id, AdventurerS1.name).filter(AdventurerS1.id.in_(member_ids)).all())
        result = []
        for (a_id, b_id), delta in changes.items():
            if delta == 0:
                continue
            entry = {'adventurer_a_id': a_id, 'adventurer_a_name': adv_names.get(a_id, '?'), 'adventurer_b_id': b_id, 'adventurer_b_name': adv_names.get(b_id, '?'), 'direction': 'up' if delta > 0 else 'down'}
            if include_delta:
                entry['delta'] = delta
            result.append(entry)
        return result

    @staticmethod
    def list_chat(page: int, per_page: int) -> Tuple[Optional[dict], Optional[str]]:
        """分页读取聊天（倒序，最多保留 HISTORY_LIMIT 条）。

        用 SQL OFFSET/LIMIT 分页，避免把 history_limit 条全部加载到 Python 再切片。
        """
        history_limit = int(current_app.config.get('ADVENTURER_S1_CHAT_HISTORY_LIMIT', 100))
        offset = (page - 1) * per_page
        if offset >= history_limit:
            return ({'messages': [], 'total': history_limit, 'page': page, 'per_page': per_page, 'has_next': False}, None)
        recent_ids_subq = AdventurerS1ChatMessage.query.with_entities(AdventurerS1ChatMessage.id).order_by(AdventurerS1ChatMessage.created_at.desc()).limit(history_limit).subquery()
        page_items = AdventurerS1ChatMessage.query.join(recent_ids_subq, AdventurerS1ChatMessage.id == recent_ids_subq.c.id).options(joinedload(AdventurerS1ChatMessage.user)).order_by(AdventurerS1ChatMessage.created_at.desc()).offset(offset).limit(per_page).all()
        total = db_manager.query(func.count()).select_from(recent_ids_subq).scalar() or 0
        return ({'messages': [m.to_dict() for m in page_items], 'total': total, 'page': page, 'per_page': per_page, 'has_next': offset + per_page < total}, None)

    @staticmethod
    def post_chat(user_id: int, content: str) -> Tuple[Optional[dict], Optional[str]]:
        """发表一条聊天，带字数与冷却校验。"""
        content = (content or '').strip()
        if not content:
            return (None, '内容不能为空')
        max_len = int(current_app.config.get('ADVENTURER_S1_CHAT_MAX_LENGTH', 300))
        if len(content) > max_len:
            return (None, f'内容不能超过 {max_len} 字')
        cooldown = 0
        if cooldown > 0:
            last = AdventurerS1ChatMessage.query.filter_by(user_id=user_id).order_by(AdventurerS1ChatMessage.created_at.desc()).first()
            if last and last.created_at:
                elapsed = (_portable_utc_now() - last.created_at).total_seconds()
                if elapsed < cooldown:
                    remaining = int(cooldown - elapsed) + 1
                    return (None, f'发言太频繁，请 {remaining} 秒后再试')
        new_id = None
        try:
            with db_manager.transaction():
                msg = AdventurerS1ChatMessage(user_id=user_id, content=content)
                ok, err = db_manager.add(msg)
                if not ok:
                    raise RuntimeError(err)
                new_id = msg.id
        except Exception as e:
            return (None, player_operation_failure(e, '发送失败'))
        saved = AdventurerS1ChatMessage.query.session.get(AdventurerS1ChatMessage, new_id)
        try:
            from plugins.adventurer_s1.services.s1_socket_service import s1_socketio as socketio
            socketio.emit('adventurer_chat_message', {'action': 'new_message', 'data': saved.to_dict()}, namespace='/')
        except Exception:
            pass
        return (saved.to_dict(), None)

    @staticmethod
    def follow(user_id: int, adventurer_id: int) -> Tuple[Optional[dict], Optional[str]]:
        """关注冒险者（幂等：已关注则视为成功）。"""
        adv = AdventurerS1.query.session.get(AdventurerS1, adventurer_id)
        if not adv:
            return (None, '冒险者不存在')
        existing = AdventurerS1Follow.query.filter_by(user_id=user_id, adventurer_id=adventurer_id).first()
        if existing:
            return ({'adventurer_id': adventurer_id, 'is_followed': True}, None)
        try:
            with db_manager.transaction():
                ok, err = db_manager.add(AdventurerS1Follow(user_id=user_id, adventurer_id=adventurer_id))
                if not ok:
                    raise RuntimeError(err)
        except Exception as e:
            return (None, player_operation_failure(e, '关注失败'))
        from plugins.adventurer_s1.services.adventurer_s1_service import invalidate_dormitory_user_cache
        invalidate_dormitory_user_cache(user_id)
        return ({'adventurer_id': adventurer_id, 'is_followed': True}, None)

    @staticmethod
    def unfollow(user_id: int, adventurer_id: int) -> Tuple[Optional[dict], Optional[str]]:
        """取消关注（幂等：未关注则视为成功）。"""
        existing = AdventurerS1Follow.query.filter_by(user_id=user_id, adventurer_id=adventurer_id).first()
        if not existing:
            return ({'adventurer_id': adventurer_id, 'is_followed': False}, None)
        try:
            with db_manager.transaction():
                db_manager.delete_instance(existing)
        except Exception as e:
            return (None, player_operation_failure(e, '取消关注失败'))
        from plugins.adventurer_s1.services.adventurer_s1_service import invalidate_dormitory_user_cache
        invalidate_dormitory_user_cache(user_id)
        return ({'adventurer_id': adventurer_id, 'is_followed': False}, None)

    @staticmethod
    def _tavern_level_requirement(target_level: int) -> Optional[int]:
        """读取升至 target_level 所需的全服存活冒险者数；未配置则返回 None。"""
        if target_level <= 1:
            return 0
        req_map = {2: int(current_app.config.get('ADVENTURER_S1_TAVERN_LEVEL2_ADVENTURER_REQ', 72))}
        return req_map.get(target_level)

    @staticmethod
    def get_tavern_upgrade_progress() -> Tuple[dict, Optional[str]]:
        """酒馆升级进度：当前等级、下一档门槛、建筑 2 级改造是否已解锁。"""
        cached = _tavern_upgrade_cache.get('progress')
        if cached is not None:
            return (cached, None)
        from plugins.adventurer_s1.services.adventurer_s1_settings_service import S1SettingsService
        tavern_level = int(S1SettingsService.get('tavern_level') or 1)
        max_level = int(current_app.config.get('ADVENTURER_S1_TAVERN_MAX_LEVEL', 2))
        alive_count = AdventurerS1.query.filter(AdventurerS1.status.notin_(AdventurerS1.TAVERN_EXCLUDED_STATUSES)).count()
        next_level = tavern_level + 1 if tavern_level < max_level else None
        next_requirement = AdventurerS1TavernService._tavern_level_requirement(next_level) if next_level is not None else None
        if next_level is None:
            progress_current = alive_count
            progress_required = alive_count
            progress_percent = 100
            requirement_met = True
            awaiting_admin_unlock = False
        else:
            progress_required = int(next_requirement or 0)
            progress_current = min(alive_count, progress_required)
            progress_percent = 100 if progress_required <= 0 else min(100, int(progress_current * 100 / progress_required))
            requirement_met = alive_count >= progress_required
            awaiting_admin_unlock = requirement_met
        building_upgrade_unlocked = tavern_level >= 2
        upgrade_ui_enabled = bool(S1SettingsService.get('tavern_upgrade_ui_enabled'))
        payload = {'tavern_level': tavern_level, 'max_level': max_level, 'alive_adventurer_count': alive_count, 'next_level': next_level, 'next_level_requirement': next_requirement, 'progress_current': progress_current, 'progress_required': progress_required, 'progress_percent': progress_percent, 'requirement_met': requirement_met, 'awaiting_admin_unlock': awaiting_admin_unlock, 'building_upgrade_unlocked': building_upgrade_unlocked, 'building_max_level': 2 if building_upgrade_unlocked else 1, 'upgrade_ui_enabled': upgrade_ui_enabled}
        _tavern_upgrade_cache.set('progress', payload)
        return (payload, None)

    @staticmethod
    def admin_unlock_tavern_upgrade(admin_user_id: int) -> Tuple[Optional[dict], Optional[str]]:
        """管理员在升级条件满足后解锁酒馆升级（写入 tavern_level）。"""
        from plugins.adventurer_s1.services.adventurer_s1_settings_service import S1SettingsService
        progress, err = AdventurerS1TavernService.get_tavern_upgrade_progress()
        if err:
            return (None, err)
        next_level = progress.get('next_level')
        if next_level is None:
            return (None, '酒馆已达当前最高等级')
        if not progress.get('requirement_met'):
            req = progress.get('next_level_requirement')
            alive = progress.get('alive_adventurer_count')
            return (None, f'升级条件未满足：需要 {req} 名存活冒险者（当前 {alive}）')
        ok, set_err = S1SettingsService.set('tavern_level', next_level, updated_by_user_id=admin_user_id)
        if not ok:
            return (None, set_err or '写入酒馆等级失败')
        AdventurerS1TavernService._provision_tavern_lv2_buildings(next_level)
        AdventurerS1TavernService._invalidate_buildings_cache_after_tavern_level_change()
        updated, err = AdventurerS1TavernService.get_tavern_upgrade_progress()
        if err:
            return (None, err)
        return ({'tavern_level': updated['tavern_level'], 'building_upgrade_unlocked': updated['building_upgrade_unlocked']}, None)

    @staticmethod
    def _invalidate_buildings_cache_after_tavern_level_change() -> None:
        """酒馆等级变更后失效建筑列表缓存（Lv2 预览占位 ↔ 真实实例切换）。"""
        invalidate_tavern_upgrade_cache()
        from plugins.adventurer_s1.services.adventurer_s1_service import invalidate_buildings_list_cache
        invalidate_buildings_list_cache()

    @staticmethod
    def _provision_tavern_lv2_buildings(level) -> None:
        """酒馆升至 Lv2 后补建仅 Lv2 解锁的建筑实例（iron_mine / smeltery / clinic 等）。"""
        try:
            from plugins.adventurer_s1.seed import seed_building_instances, sync_building_instance_states
            seed_building_instances()
            sync_building_instance_states()
        except Exception:
            current_app.logger.exception('补建酒馆 Lv%s 建筑实例失败', level)

    @staticmethod
    def admin_force_set_tavern_level(admin_user_id: int, target_level: Optional[int]=None) -> Tuple[Optional[dict], Optional[str]]:
        """GM 测试用：无视升级门槛直接设置酒馆等级（默认升到最高），并补建 Lv2 建筑。"""
        from plugins.adventurer_s1.services.adventurer_s1_settings_service import S1SettingsService
        progress, err = AdventurerS1TavernService.get_tavern_upgrade_progress()
        if err:
            return (None, err)
        max_level = int(progress.get('max_level') or 2)
        if target_level is None:
            target_level = max_level
        target_level = max(1, min(int(target_level), max_level))
        ok, set_err = S1SettingsService.set('tavern_level', target_level, updated_by_user_id=admin_user_id)
        if not ok:
            return (None, set_err or '写入酒馆等级失败')
        if target_level >= 2:
            AdventurerS1TavernService._provision_tavern_lv2_buildings(target_level)
        AdventurerS1TavernService._invalidate_buildings_cache_after_tavern_level_change()
        updated, err = AdventurerS1TavernService.get_tavern_upgrade_progress()
        if err:
            return (None, err)
        return ({'tavern_level': updated['tavern_level'], 'building_upgrade_unlocked': updated['building_upgrade_unlocked']}, None)
