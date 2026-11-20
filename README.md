# 实时数仓CDC变更捕获中心

面向实时数仓建设场景，提供Binlog解析、变更事件捕获、DDL追踪、SCD处理和下游分发同步的CDC治理平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| Binlog解析 | `binlog_parser` | 解析配置 | 配置名称、源数据库、解析模式 |
| 变更事件 | `change_event` | 变更记录 | 事件ID、来源表、操作类型 |
| DDL追踪 | `ddl_tracker` | DDL记录 | 记录ID、变更数据库、变更表名 |
| Schema同步 | `schema_sync` | 同步任务 | 任务名称、源表、目标表 |
| 采集通道 | `cdc_channel` | 通道实例 | 通道名称、Binlog源、输出目标 |
| 检查点管理 | `checkpoint_manager` | 检查点 | 检查点ID、所属通道、Binlog文件 |
| 位点存储 | `position_store` | 位点记录 | 记录ID、数据库实例、Binlog名称 |
| 格式适配 | `format_adapter` | 适配规则 | 规则名称、输入格式、输出格式 |
| 缓慢变化维 | `scd_handler` | SCD配置 | 配置名称、维度表、SCD类型 |
| 去重窗口 | `dedup_window` | 窗口配置 | 配置名称、去重键、窗口大小 |
| 路由分发 | `route_dispatcher` | 路由规则 | 规则名称、匹配条件、目标通道 |
| 回填位点 | `backfill_position` | 回填任务 | 任务名称、源位点、目标位点 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标时间、所属通道、延迟秒数 |
| 快照比对 | `snapshot_compare` | 比对报告 | 报告名称、源表名、目标表名 |
| 事件过滤 | `event_filter` | 过滤规则 | 规则名称、过滤类型、匹配表达式 |
| 分区同步 | `partition_sync` | 同步配置 | 配置名称、源分区策略、目标分区策略 |
| 审计轨迹 | `audit_trail` | 审计日志 | 审计时间、操作类型、目标对象 |
| 故障切换 | `failover_group` | 切换组 | 组名、主通道、备用通道 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 去重窗口与变更事件消费

去重按**事件自身的事件时间**划分有效窗口，与提交/到达顺序无关，因此换序提交、
补入迟到记录和整体重放都会得到同一份去重明细。

- 窗口区间为 `[start, start + 窗口大小)`；同一去重键可同时存在相邻甚至重叠的
  窗口（如 `10:00:00–10:01:00` 与 `10:00:40–10:01:40`），各自保留窗口内记录。
- 保留最早/最晚只在**单个窗口内部**生效（`first`/`retain_first` 与
  `last`/`retain_last`），窗口之间互不影响。
- 同一源坐标只归属一个窗口；重叠区的迟到记录按“锚定窗口优先，其次窗口起点
  最早且尚未交付”裁决，并留下归属冲突记录。已交付窗口不再接收新记录，
  迟到记录另开新窗口，已交付镜像仍可按当时的窗口复核。
- 变更事件直接消费去重后的变更明细：每消费一条就落库源坐标回执，执行结果
  回写去重清单；同一源坐标重复提交只记一笔；重放时按回执跳过已交付事件。

相关接口（`backend/app/routers/dedup.py`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/dedup/windows` | 登记可选窗口 + 按事件时间去重，返回去重清单与窗口复核视图 |
| GET | `/api/dedup/windows?key=` | 查看某去重键的窗口、归属与交付状态 |
| GET | `/api/dedup/manifest` | 去重后的变更明细（`?only_pending=true` 只看未交付） |
| POST | `/api/dedup/consume` | 变更事件消费去重清单，落库回执并回写执行结果 |
| POST | `/api/dedup/replay` | 重放去重清单，已交付坐标跳过 |

回归验证：`cd backend && .venv/bin/python tests/test_dedup_pipeline.py`

领域：数据工程
子方向：数仓与数据湖
