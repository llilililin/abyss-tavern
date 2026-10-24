# Release 1.0.0

深渊酒馆（Abyss Tavern）`v1.0.0`。

本版基于 `v0.2.0`，包含一项针对管理员视图的显示修复。

---

## 修复：管理员视角探险成员显示为「冒险者 #id」

### 现象

管理员查看探险日志时，成员卡片不显示角色名，而是显示 `冒险者 #25` 这样的编号。普通玩家视角正常。

### 原因

探险日志的 `full_log.member_updates` 中，生成阶段只写入了 `id`，没有写入 `name`：

```json
{ "id": 25, "name": null, "is_dead": true, ... }
```

后端存在两套日志视图：

| 视图 | 处理路径 | 补名 |
| --- | --- | --- |
| 玩家 | `build_player_s1_full_log` | 有，显示正常 |
| 管理员 | `log.to_dict(is_admin=True)` | 无，直接输出原始数据 |

前端成员卡片 `ExpeditionMemberUpdates` 读取 `memberUpdates[].name`，取不到时回退：

```js
function ee(e){ return e.name || `冒险者 #${e.id}` }
```

因此只有管理员会看到编号。

### 修复

在 `adventurer_s1_tavern_service.py` 新增 `_backfill_member_update_names()`，于 `get_log_detail()` 返回前统一补齐缺失的 `name`。

```python
data['members'] = resolve_expedition_log_member_names(log, raw_full_log, data.get('full_log'))
_backfill_member_update_names(data.get('full_log'))
```

要点：

- 收集所有缺名且带合法 `id` 的成员，一次性查询 `adventurer_s1`，避免逐条查库。
- 玩家与管理员视图都会经过此处，行为一致。
- 补名在读取时动态完成，**历史日志无需回填数据**。
- 角色已被彻底删除时保留编号回退，属预期行为。

### 改动范围

单文件，+48 行，无删除。

```
game-packs/s1-fantasy/source-snapshot/backend/plugins/
  adventurer_s1/services/adventurer_s1_tavern_service.py
```

---

## 部署

```bash
cp .env.example .env          # 填入 SECRET_KEY / JWT_SECRET_KEY / RUNTIME_BOOTSTRAP_SECRET
python -m venv .venv
.venv/bin/python -m pip install -r backend/requirements.txt
.venv/bin/python backend/serve.py
```

默认监听 `:5500`，访问 `http://127.0.0.1:5500`。

升级已有部署时，只需更新上述单个文件并重启服务。

---

## 许可

见 [LICENSE](LICENSE) 与 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。