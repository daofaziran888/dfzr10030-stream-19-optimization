# 管道工坊 · 实时数仓与流式管道平台

面向实时数据接入、流式转换、变更捕获、窗口聚合、状态管理、断流恢复与多维数据集下发的实时数仓与流式管道开发平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
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
| 源端绑定 | `source_binding` | 数据源端 | 源端编号、源端类型、连接地址 |
| 变更捕获 | `cdc_task` | CDC任务 | 任务编号、源数据库、监控表 |
| 流式转换 | `stream_transform` | 转换算子 | 算子编号、输入流、输出流 |
| 窗口聚合 | `window_agg` | 时间窗口 | 窗口编号、聚合指标、窗口类型 |
| 状态管理 | `state_store` | 算子状态 | 状态编号、关联算子、存储后端 |
| 规则引擎 | `rule_engine` | 业务规则 | 规则编号、规则条件、触发动作 |
| 水位管理 | `watermark_mgr` | 事件水位 | 水位编号、关联流、水位类型 |
| 数据落盘 | `sink_writer` | 输出槽位 | 槽位编号、输出类型、目标地址 |
| 背压控制 | `backpressure` | 背压策略 | 策略编号、关联算子、队列上限 |
| 断流恢复 | `checkpoint` | 检查点 | 检查点编号、关联流水线、位点时间 |
| 流式质检 | `data_quality` | 质检规则 | 规则编号、关联流、检查项 |
| Schema演进 | `schema_evolve` | 结构变更 | 变更编号、关联流、变更类型 |
| 管道编排 | `pipeline_orch` | 管道DAG | 管道编号、管道名称、节点列表 |
| 去重管理 | `dedup_mgr` | 去重策略 | 策略编号、关联流、去重键 |
| 指标计算 | `metric_calc` | 实时指标 | 指标编号、指标名称、计算公式 |
| 事件路由 | `event_router` | 路由规则 | 规则编号、事件类型、匹配条件 |
| 历史回放 | `replay_batch` | 回放任务 | 任务编号、数据源、起始时间 |
| 分组分区 | `grp_partition` | 分组策略 | 策略编号、关联流、分区键 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 阈值审计导出（流式质检）

历史导出 `GET /api/data_quality/export` 仍只含规则行，列顺序不变；样本、执行批次和
执行时实际适用的阈值放在独立的阈值审计包里，由 `app/services/quality_audit.py` 维护。

调用链：`data_quality` 规则 → 质量样本（`fixtures.QUALITY_SAMPLES` 的采集口径）
→ 阈值版本 → 执行批次 → 审计包 → `pipeline_orch` 复核。

| 接口 | 作用 |
| --- | --- |
| `GET /api/data_quality/audit/rules` | 规则与当前阈值版本 |
| `GET/POST /api/data_quality/audit/rules/{rule}/thresholds` | 查看 / 追加阈值版本（旧版本保留） |
| `GET /api/data_quality/audit/samples` | 质量样本、样本来路、采集时间、缺测原因 |
| `POST /api/data_quality/audit/executions` | 建执行批次，冻结当时生效的阈值快照 |
| `POST /api/data_quality/audit/executions/{id}/seal` | 封包，固化截止前到达的样本 |
| `POST /api/data_quality/audit/executions/{id}/samples` | 补录迟到样本（标记迟到、不计入统计） |
| `GET /api/data_quality/audit/executions/{id}/package` | 下载审计包 CSV/JSON |
| `GET /api/data_quality/audit/executions/{id}/manifest` | 包的封包口径与完整度 |
| `POST /api/data_quality/audit/executions/{id}/replay` | 读包重算并把复核结果回写审计清单 |
| `GET /api/data_quality/audit/reviews` | 审计清单（含导出时的证据位置） |
| `POST /api/pipeline/quality-replay` | 管道编排触发同一条复核管道 |

审计包每行都带 `执行批次`、`采集时间`、`证据位置`；样本行另外记录 `当时阈值版本`
与当时的上下限。`当时*` 字段冻结不可改，`当前*` 字段用于说明按今天的阈值会怎么判，
因此先改阈值再审计旧批次时，旧判断仍然可解释。缺测行 `是否计入统计=否`，
迟到补录行 `迟到=是`、`是否计入统计=否`，都不会被算成合格记录。

`GET .../package?interrupt_after=N` 模拟落盘中断：已写行进入归档并返回 503，
重新请求同一执行从断点续写；同一执行重复导出、补录或复核都不会产生重复审计行。

领域：数据工程
子方向：数仓与数据湖
