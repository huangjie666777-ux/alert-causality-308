# alert-causality-308：指标抓取与持续阈值告警后端

定期并发抓取 Prometheus 文本格式（0.0.4）的 gauge 指标，按"值 > 阈值且持续
满足"的状态机产生告警，支持按规则依赖抑制连带告警并追溯根因，把每轮状态、
抑制快照与事件事务性写入 SQLite，并提供只读 HTTP 查询 API。无 PromQL、无通
知、无前端。

## 环境

Python 3.14，依赖已安装在仓库 `.venv` 中（aiohttp 3.13.3、
prometheus-client 0.22.1、pytest）。所有命令均使用 `.venv/bin/python`。

## 快速开始

```bash
# 校验配置（启动校验，不启动服务）
.venv/bin/python -m causewatch_308.main --check --config config.json

# 启动演示指标源（127.0.0.1:9101，/metrics 与 /set?metric=...&value=...）
.venv/bin/python demo/exporter.py 9101 &

# 启动后端（127.0.0.1:8080）
.venv/bin/python -m causewatch_308.main --config config.json
```

用 curl 演示触发与恢复（规则：温度 > 30 持续 3 秒）：

```bash
curl -s http://127.0.0.1:8080/api/targets                              # 目标健康
curl -s http://127.0.0.1:8080/api/targets/demo-exporter/samples        # 最新样本
curl -s "http://127.0.0.1:9101/set?metric=demo_temperature_celsius&value=35"
curl -s http://127.0.0.1:8080/api/alerts                               # pending → firing
curl -s "http://127.0.0.1:8080/api/events"                             # firing 事件
curl -s "http://127.0.0.1:9101/set?metric=demo_temperature_celsius&value=20"
curl -s "http://127.0.0.1:8080/api/events?after_id=1&limit=10"         # resolved/recovered
```

用 curl 演示连带抑制与重新暴露（湿度 > 80 持续 3 秒，被温度告警抑制）：

```bash
# 下游湿度告警先触发：出现在 actionable 中
curl -s "http://127.0.0.1:9101/set?metric=demo_humidity_percent&value=90"
sleep 5
curl -s http://127.0.0.1:8080/api/alerts            # actionable = [demo-humidity-high]

# 上游温度告警触发：湿度告警被抑制，actionable 只剩根因
curl -s "http://127.0.0.1:9101/set?metric=demo_temperature_celsius&value=35"
sleep 5
curl -s http://127.0.0.1:8080/api/alerts            # humidity suppressed=true，
                                                    # suppressed_by/root_causes 指向温度
curl -s http://127.0.0.1:8080/api/events            # firing, firing, suppressed

# 上游恢复：湿度告警解除抑制，重新出现在 actionable 中
curl -s "http://127.0.0.1:9101/set?metric=demo_temperature_celsius&value=20"
sleep 5
curl -s http://127.0.0.1:8080/api/events            # + resolved/recovered, unsuppressed
curl -s http://127.0.0.1:8080/api/alerts            # actionable 重新包含 demo-humidity-high
```

运行测试：

```bash
.venv/bin/python -m pytest
```

## 配置（JSON）

```json
{
  "host": "127.0.0.1",            // 可选，默认 0.0.0.0
  "port": 8080,                   // 本机 HTTP 端口，必填
  "sqlite_path": "data/alerts.db",// SQLite 路径，必填（自动建目录）
  "targets": [
    {
      "id": "demo-exporter",                 // 唯一 ID
      "url": "http://127.0.0.1:9101/metrics",// http(s) 地址
      "interval_seconds": 1,                 // 抓取周期 > 0
      "timeout_seconds": 2,                  // 单次抓取超时 > 0
      "max_response_bytes": 1048576      // 响应字节上限，正整数
    }
  ],
  "rules": [
    {
      "id": "demo-temperature-high",     // 唯一 ID
      "target_id": "demo-exporter",      // 必须引用已存在的目标
      "metric": "demo_temperature_celsius",
      "labels": {"room": "server"},      // 标签等值筛选，可空 {}
      "threshold": 30,                   // 有限数值（拒绝 NaN/Inf）
      "duration_seconds": 3              // 非负持续秒数，0 表示立即触发
    },
    {
      "id": "demo-humidity-high",
      "target_id": "demo-exporter",
      "metric": "demo_humidity_percent",
      "labels": {"room": "server"},
      "threshold": 80,
      "duration_seconds": 3
    }
  ],
  "dependencies": [                      // 可选，规则间抑制依赖
    {
      "upstream": "demo-temperature-high",   // 上游规则，必须引用已存在的规则
      "downstream": "demo-humidity-high",    // 下游规则，必须引用已存在的规则
      "labels": ["room"]                     // 非空关联标签列表
    }
  ]
}
```

启动时全量校验：未知字段、重复 ID、非法指标/标签名、非有限阈值、负数持续
时间、引用不存在的目标等都会以明确报错拒绝启动（退出码 2）。依赖额外校验：
引用不存在的规则、规则自依赖、重复边（同一上游→下游对，与标签无关）、依赖
环（直接或间接）以及空的关联标签列表都会被拒绝；不配置 `dependencies` 时行
为与旧版完全一致。

## 抓取与解析

- 每个目标一个独立 asyncio 任务：目标内轮次严格串行不重叠，目标间互不阻塞；
  共享一个连接池，退出时取消任务并关闭连接。
- 解析器支持 0.0.4 的 gauge（及 untyped）样本与标签转义（`\n`、`\"`、`\\`）。
  其他已声明类型（counter/histogram/summary）的样本会被完整校验后跳过。
- 以下情况使**整轮失败**，不发布任何半份样本（保留上一轮成功样本）：
  格式错误、重复序列（标签乱序算同一序列）、非有限值（NaN/±Inf）、显式时间戳、
  HTTP 非 200、超时、响应超过字节上限（Content-Length 与实际读取双重检查）。

## 告警语义

- 告警身份 = 规则 ID + 序列完整标签集；标签乱序不改变身份。
- 值**严格大于**阈值进入 `pending`；在单调时钟上持续满足 `duration_seconds`
  才转为 `firing`；`duration_seconds = 0` 当轮立即触发。持续异常不重复触发。
- 值回落、序列消失、抓取失败分别清除 `pending`（静默）或将 `firing` 恢复一次，
  原因分别为 `recovered` / `series_missing` / `scrape_failed`。
- 再次超限从零重新计时，绝不跨失败轮/恢复累计。
- 每轮的抓取结果、样本替换、全部目标的活动告警镜像（含抑制快照）与该轮的
  触发/恢复/抑制/解除事件在**同一 SQLite 事务**中写入；事件 ID 为自增主键，
  稳定且单调，供按 ID 翻页。
- 内存状态只在落库成功后推进：先规划整轮变更，写入失败则内存不变、事件不丢，
  下一轮重新规划，绝不出现"内存已变更但事件漏记"。
- 重启时保留全部历史（轮次/样本/事件），清除所有 `pending`，遗留的 `firing`
  一次性以 `resolved`/`restart` 恢复，抑制状态随之清除；停机时间不累计，恢复
  后重新计时。旧版本创建的数据库在打开时自动补齐抑制列，历史数据保留。

## 依赖抑制与根因

- 依赖在规则之间声明：`upstream` 规则的 firing 实例抑制与其相连的
  `downstream` 规则 firing 实例。
- 两个 firing 实例相连，当且仅当依赖声明的**全部关联标签在两侧都存在且相
  等**；任一侧缺少标签即不匹配，标签顺序无关，实例身份仍由完整标签集区分。
- 每轮结束后按**所有目标**的活动告警重算抑制：任一目标的轮次都可能改变其
  他目标告警的抑制状态。
- 抑制可传递：被抑制的实例继续抑制自己的下游；`pending` 实例既不能抑制别
  人也不会被抑制（下游计时不受影响，到点照常 firing 并立即被抑制）。
- 抑制只是覆盖层：不停止下游计时，不删除原有的触发/恢复事件。
- 仍 firing 的实例**首次**被抑制时追加一条 `suppressed` 事件，解除时追加一
  条 `unsuppressed` 事件；状态不变不重复。存在多个上游时，**全部**上游解除
  才释放；下游自身恢复（resolved）不产生解除事件。
- 重启沿用 `restart` 结算并清除抑制，不产生解除事件，历史事件全部保留。
- `/api/alerts` 中每个被抑制告警携带 `suppressed_by`（直接来源实例）与
  `root_causes`（沿当前实例关系追溯到的全部未抑制根因），引用均保留规则、
  目标与完整标签集。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/targets` | 全部目标配置与健康（ok、error、last_scrape_at、duration_ms、sample_count、consecutive_failures） |
| GET | `/api/targets/{id}/samples` | 该目标最新一轮成功样本（未知目标 404） |
| GET | `/api/alerts` | 全部活动告警（pending/firing、since、当前值、阈值、完整标签集、`suppressed`、`suppressed_by`、`root_causes`）及 `actionable`（可处置的未抑制 firing 子集） |
| GET | `/api/events?after_id=0&limit=50` | 事件按 ID 升序翻页，返回 `next_after_id` 与 `has_more`；limit 1–500；kind 为 `firing`/`resolved`/`suppressed`/`unsuppressed` |

错误统一返回 `{"error": "..."}` 与相应 4xx 状态码。

## 代码结构

```
causewatch_308/
  config.py    # JSON 配置加载与启动校验
  labels.py    # 标签集规范化（排序身份，乱序不变）
  parser.py    # 0.0.4 文本格式解析（gauge、标签转义、严格校验）
  scraper.py   # 每目标并发抓取循环、超时与字节上限
  state.py     # pending/firing/resolved 状态机（单调时钟，先规划后提交）
  deps.py      # 依赖推导：实例连接、抑制覆盖与根因追溯
  store.py     # SQLite：同事务写轮次/样本/告警/抑制/事件，重启恢复，旧库迁移
  engine.py    # 抓取结果 → 状态机+抑制 → 落库成功才提交内存 的粘合层
  server.py    # 只读 HTTP 查询 API
  main.py      # 装配、信号处理与优雅退出
demo/exporter.py  # 演示指标源（/metrics、/set）
tests/            # 解析器、状态机、配置、依赖推导、仓储、引擎与端到端测试
```

## 备注

- `rounds`/`events` 表只增不减，未实现保留期清理；生产使用需自行定期归档。
- 样本表仅保存每个目标最近一次成功轮次；失败轮次可通过 `/api/targets` 的
  健康字段与 `rounds` 表观察。
