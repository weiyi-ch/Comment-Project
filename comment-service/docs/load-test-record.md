# comment-service 压测记录

这份文档只记录重新压测后的真实结果。旧结果不再迁移进来。

## 记录规则

每次压测结果按下面模板追加：

```text
## RUN-001：测试名称

时间：
场景：
命令：
测试数据：
并发参数：
缓存模式：

k6 summary：

trace_id 样例：

Jaeger 观察：

SQL 反查：

问题判断：

后续动作：
```

## 结果总览

| RUN | 场景 | VUS/时长 | 缓存模式 | requests | failed | p95 | p99 | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RUN-001 | 学生帖子详情 `student-post-detail` | 1 VU / 10s | default | 18 | 0.00% | 1047.93 ms | 1113.31 ms | 通过 |
| RUN-002 | 学生评论列表 `student-comment-list` | 1 VU / 10s | default | 26 | 0.00% | 665.74 ms | 1242.21 ms | 通过 |
| RUN-003 | 学生帖子详情随机池热缓存 `student-post-detail` | 30 VU / 30s | default | 5194 | 0.00% | 303.16 ms | 628.93 ms | 通过 |
| RUN-004 | 学生帖子详情随机池热缓存第二轮 `student-post-detail` | 30 VU / 30s | default | 5159 | 0.00% | 300.50 ms | 580.26 ms | 通过 |
| RUN-005 | 学生帖子详情随机池热缓存第三轮 `student-post-detail` | 30 VU / 30s | default | 5230 | 0.00% | 293.88 ms | 618.72 ms | 通过 |
| RUN-006 | 学生帖子详情二八热点无预热 `student-post-detail` | 30 VU / 30s | default | 7134 | 0.00% | 254.53 ms | 374.49 ms | 通过 |
| RUN-007 | 学生帖子详情二八热点 hash 选热点 `student-post-detail` | 30 VU / 30s | default | 5056 | 0.00% | 298.15 ms | 540.78 ms | 通过 |
| RUN-008 | 学生帖子详情二八热点 hash 第二轮 `student-post-detail` | 30 VU / 30s | default | 4916 | 0.00% | 310.99 ms | 531.03 ms | 通过 |
| RUN-009 | 学生帖子详情二八热点 bypass `student-post-detail` | 10 VU / 30s | bypass | 1284 | 0.00% | 558.25 ms | 642.55 ms | 通过 |
| RUN-010 | 学生帖子搜索随机关键词第一轮 `student-post-search` | 20 VU / 30s | default | 10987 | 0.00% | 65.12 ms | 88.63 ms | 通过 |
| RUN-011 | 学生帖子搜索随机关键词第二轮 `student-post-search` | 20 VU / 30s | default | 11034 | 0.00% | 65.09 ms | 74.14 ms | 通过 |
| RUN-012 | 学生评论搜索二八帖子池第一轮 `student-comment-search` | 20 VU / 30s | default | 9933 | 0.00% | 150.12 ms | 182.15 ms | 通过 |
| RUN-013 | 学生评论搜索二八帖子池第二轮 `student-comment-search` | 20 VU / 30s | default | 10669 | 0.00% | 67.59 ms | 82.08 ms | 通过 |
| RUN-014 | 单热点帖子并发点赞 `like-post` | 30 VU / 20s | default | 398 | 8.79% | 3058.09 ms | 3059.72 ms | 故障定位，已改造待复测 |

## 流量模型说明

- `tools/k6/comment-service.js` 当前 `POST_ID_MODE` 支持 `round_robin`、`random` 和 `hotspot_80_20`。
- RUN-003、RUN-004、RUN-005 均为 `post_id_mode=random`，表示 47 个帖子均匀随机选择。
- 这三轮可以作为“均匀随机帖子池热缓存基准”，证明热缓存读链路在 30 VU 下较稳定。
- 这三轮不能代表真实生产流量；真实流量通常有热点倾斜、长尾帖子、用户停留和重复访问，不是均匀随机，也不是 `round_robin`。
- RUN-006 为 `post_id_mode=hotspot_80_20`，不做预热；热点帖子不是服务端已知配置，而是压测脚本用访问概率构造出来的二八流量。
- 后续 `hotspot_80_20` 默认用 `HOT_POST_PICK=hash` 从帖子池稳定抽取合成热点，避免依赖 `POST_IDS` 顺序。

## 详细记录

## RUN-001：5.1 Smoke Test - 学生帖子详情

时间：2026-05-27 14:51 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=1 \
DURATION=10s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=3 \
REQUEST_ID_PREFIX=smoke-detail \
SUMMARY_PATH=reports/k6/smoke-detail.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量；`round_robin` 模式下本轮 18 次请求覆盖帖子池中的 18 次选择
- `page_size=20`
- `post_id_mode=round_robin`

并发参数：

- `VUS=1`
- `DURATION=10s`
- 实际运行约 `10.3s`
- 完成 `18` 次请求，`0` 次中断

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 18 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 570.70 ms |
| latency_p50 | 337.54 ms |
| latency_p90 | 999.05 ms |
| latency_p95 | 1047.93 ms |
| latency_p99 | 1113.31 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `smoke-detail-student-post-detail-vu1-0` | `dd90a1627a6d94cd6b6be57e06806f82` | 200 | 535.9 ms |
| `smoke-detail-student-post-detail-vu1-1` | `7f43f42040fbd41c7971cdc8e91d71a6` | 200 | 331.587 ms |
| `smoke-detail-student-post-detail-vu1-2` | `6edd682918a2d9824ec7400ca057ac8d` | 200 | 334.272 ms |

Jaeger 观察：

- 控制台已返回 trace_id，满足 5.1 Smoke Test 的 trace 输出要求。
- 尚未补充 Jaeger UI 截图或 span 细节。

SQL 反查：

- 本轮为 smoke 跑通验证，未做 SQL 反查。

问题判断：

- 5.1 帖子详情 smoke 通过。
- `http_failed=0.00%`，`checks_rate=100.00%`，接口可用性正常。
- `p95=1047.93ms < 1500ms`，`p99=1113.31ms < 3000ms`，满足本轮阈值。
- 当前是 1 VU 小流量验证，只能证明链路跑通，不能代表并发性能上限。

后续动作：

- 继续执行 5.2 评论列表 smoke。
- 之后再进入多 VU/多帖子 ID 池压测，观察缓存命中、MySQL 回源和 Jaeger span 耗时分布。

## RUN-002：5.2 Smoke Test - 学生评论列表

时间：2026-05-27 14:57 Asia/Shanghai

场景：`student-comment-list`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-comment-list \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=1 \
DURATION=10s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=3 \
REQUEST_ID_PREFIX=smoke-comment-list \
SUMMARY_PATH=reports/k6/smoke-comment-list.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=round_robin`
- `page_size=20`
- 已设置帖子池和评论池；当前 `student-comment-list` 场景只使用帖子池，不使用评论池

并发参数：

- `VUS=1`
- `DURATION=10s`
- 实际运行约 `11.0s`
- 完成 `26` 次请求，`0` 次中断

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 26 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 422.80 ms |
| latency_p50 | 369.26 ms |
| latency_p90 | 476.35 ms |
| latency_p95 | 665.74 ms |
| latency_p99 | 1242.21 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `smoke-comment-list-student-comment-list-vu1-0` | `276cc185bf2e86ac7c5d7151d3b39c71` | 200 | 338.598 ms |
| `smoke-comment-list-student-comment-list-vu1-1` | `2237e5ccd46f6761c83adede97c6b47e` | 200 | 709.697 ms |
| `smoke-comment-list-student-comment-list-vu1-2` | `555bc503ca2fbb12c6957a4e4f2bbc5b` | 200 | 342.011 ms |

Jaeger 观察：

- 控制台已返回 trace_id，满足 5.2 Smoke Test 的 trace 输出要求。
- 后续可重点查看 `redis.GET student:post_comments`、`redis.MGET mysql:comment`、`mysql.SELECT study_comment.list` 或 `batch_by_ids`。

SQL 反查：

- 本轮为 smoke 跑通验证，未做 SQL 反查。

问题判断：

- 5.2 学生评论列表 smoke 通过。
- `http_failed=0.00%`，`checks_rate=100.00%`，接口可用性正常。
- `p95=665.74ms < 1500ms`，`p99=1242.21ms < 3000ms`，满足本轮阈值。
- 相比 RUN-001 帖子详情，本轮平均延迟和 p95 更低，符合评论列表链路比详情聚合链路更轻的预期。
- 当前是 1 VU 小流量验证，只能证明链路跑通，不能代表并发性能上限。

后续动作：

- 第一阶段 5.1、5.2 smoke 已通过。
- 下一步可以进入第二阶段读链路压测：单热点热缓存、多帖子池、绕过缓存对比。

## WARMUP-001：帖子详情随机帖子池预热

时间：2026-05-27 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
MAX_P95_MS=5000 \
MAX_P99_MS=8000 \
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=1 \
DURATION=5s \
REQUEST_ID_PREFIX=warmup-detail-hot \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=random`
- `page_size=20`
- 注意：本轮 request_id 前缀是 `warmup-detail-hot`，但实际使用随机帖子池，不是单热点帖子预热。

并发参数：

- `VUS=1`
- `DURATION=5s`
- 实际运行约 `5.3s`
- 完成 `10` 次请求，`0` 次中断

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 10 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 526.21 ms |
| latency_p50 | 340.87 ms |
| latency_p90 | 961.36 ms |
| latency_p95 | 976.60 ms |
| latency_p99 | 988.79 ms |
| 阈值 | `p95 < 5000ms`、`p99 < 8000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `warmup-detail-hot-student-post-detail-vu1-0` | `b20efa64800722b1ae4888ed4eb3580e` | 200 | 332.436 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看预热期间帖子详情链路。

SQL 反查：

- 预热阶段未做 SQL 反查。

问题判断：

- 预热请求全部成功，`http_failed=0.00%`，`checks_rate=100.00%`。
- 本轮阈值放宽为预热阈值，结果满足 `p95 < 5000ms`、`p99 < 8000ms`。
- 随机帖子池模式下，10 次请求会随机命中 47 个帖子中的一部分，适合粗略预热多帖子读链路。

后续动作：

- 若目标是单热点预热，应先 `unset POST_IDS` 或把 `POST_IDS` 只设为 `$HOT_POST_ID`，并使用 `POST_ID_MODE=round_robin`。
- 若目标是多帖子池压测，可以继续保持 `POST_ID_MODE=random`，进入第二阶段读链路测试。

## RUN-003：预热后实测 - 学生帖子详情随机帖子池

时间：2026-05-27 15:06 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-hot \
SUMMARY_PATH=reports/k6/detail-hot.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=random`
- `page_size=20`
- 注意：本轮 request_id 前缀是 `detail-hot`，但实际使用随机帖子池，不是单热点帖子压测。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.3s`
- 完成 `5194` 次请求，`0` 次中断
- 吞吐约 `171.59 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 5194 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 173.84 ms |
| latency_p50 | 113.29 ms |
| latency_p90 | 278.93 ms |
| latency_p95 | 303.16 ms |
| latency_p99 | 628.93 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-hot-student-post-detail-vu10-1` | `54145cb2157c48192f8d12480e3dc1fd` | 200 | 687.709 ms |
| `detail-hot-student-post-detail-vu26-0` | `b97cbb980d80e224f2bfbde033bcbdca` | 200 | 1681.009 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看预热后高并发下的帖子详情链路。
- 后续可重点查看 `StudentService.GetPostDetailStudent`、`studentRepo.GetPostByID`、`redis.GET mysql:post`、`redis.GET student:post_comments`、`redis.MGET mysql:comment` 和回复批量查询相关 span。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 预热后 30 VU 实测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=303.16ms < 1500ms`，`p99=628.93ms < 3000ms`，明显低于阈值。
- 相比 1 VU smoke 和预热阶段，本轮延迟显著下降，说明随机帖子池在热缓存读路径下表现稳定。
- 当前结果代表 47 个帖子随机池的热缓存表现，不代表单热点帖子压测结果。

后续动作：

- 若目标是单热点压测，应先 `unset POST_IDS` 或设置 `POST_IDS=$HOT_POST_ID`，再用 `POST_ID_MODE=round_robin` 重新执行。
- 可继续执行 `student-comment-list` 的 30 VU 随机帖子池压测，并补一轮 `CACHE_MODE=bypass` 对比 MySQL 回源成本。

## RUN-004：第二轮复测 - 学生帖子详情随机帖子池

时间：2026-05-27 15:22 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-hot \
SUMMARY_PATH=reports/k6/detail-hot.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=random`
- `page_size=20`
- 注意：本轮 request_id 前缀是 `detail-hot`，但实际使用随机帖子池，不是单热点帖子压测。
- 注意：`SUMMARY_PATH=reports/k6/detail-hot.json` 与 RUN-003 相同，第二轮已覆盖该 JSON 文件；后续建议按轮次保存，例如 `detail-hot-r2.json`。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.3s`
- 完成 `5159` 次请求，`0` 次中断
- 吞吐约 `170.54 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 5159 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 174.84 ms |
| latency_p50 | 115.26 ms |
| latency_p90 | 281.70 ms |
| latency_p95 | 300.50 ms |
| latency_p99 | 580.26 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-hot-student-post-detail-vu24-1` | `c588dc5b30648713d7a085a46fff10bd` | 200 | 563.687 ms |
| `detail-hot-student-post-detail-vu23-0` | `dbe3e2584fe8a33755d18678f1a4f69b` | 200 | 580.491 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可继续对比 RUN-003 的关键 span。
- 后续可重点查看 `StudentService.GetPostDetailStudent`、`studentRepo.GetPostByID`、`redis.GET mysql:post`、`redis.GET student:post_comments`、`redis.MGET mysql:comment` 和回复批量查询相关 span。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 第二轮 30 VU 复测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=300.50ms < 1500ms`，`p99=580.26ms < 3000ms`，明显低于阈值。
- 与 RUN-003 对比，requests 从 `5194` 到 `5159`，吞吐从 `171.59 req/s` 到 `170.54 req/s`，p95 从 `303.16ms` 到 `300.50ms`，p99 从 `628.93ms` 到 `580.26ms`，整体波动很小。
- 两轮结果说明 47 个帖子随机池热缓存链路在 30 VU 下表现稳定。

后续动作：

- 若继续压测同一场景，建议把 `SUMMARY_PATH` 改成带轮次的文件名，避免覆盖上一轮 JSON。
- 可继续执行 `student-comment-list` 的 30 VU 随机帖子池压测，之后再补 `CACHE_MODE=bypass` 对比缓存收益。

## RUN-005：第三轮复测 - 学生帖子详情随机帖子池

时间：2026-05-27 15:23 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-hot \
SUMMARY_PATH=reports/k6/detail-hot.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=random`
- `page_size=20`
- 注意：本轮和 RUN-003、RUN-004 一样，都是均匀随机帖子池，不是 `round_robin`，也不是真实生产热点分布。
- 注意：`SUMMARY_PATH=reports/k6/detail-hot.json` 与前两轮相同，第三轮已覆盖该 JSON 文件；后续建议按轮次保存，例如 `detail-hot-r3.json`。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.2s`
- 完成 `5230` 次请求，`0` 次中断
- 吞吐约 `172.95 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 5230 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 172.47 ms |
| latency_p50 | 112.64 ms |
| latency_p90 | 275.51 ms |
| latency_p95 | 293.88 ms |
| latency_p99 | 618.72 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-hot-student-post-detail-vu5-0` | `78f4ef8d2fb6f609c6583fd8c46679e9` | 200 | 618.504 ms |
| `detail-hot-student-post-detail-vu10-1` | `80a2ce49cc277ce2e0706232b712e90a` | 200 | 1590.46 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可继续对比前三轮的关键 span。
- 第二条 trace 样例耗时 `1590.46ms`，但整体 p99 仍为 `618.72ms`，说明该样例属于极少数慢请求，后续可在 Jaeger 里单独查看慢在哪个 span。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 第三轮 30 VU 复测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=293.88ms < 1500ms`，`p99=618.72ms < 3000ms`，明显低于阈值。
- RUN-003、RUN-004、RUN-005 三轮吞吐分别为 `171.59 req/s`、`170.54 req/s`、`172.95 req/s`，p95 分别为 `303.16ms`、`300.50ms`、`293.88ms`，均匀随机热缓存基准表现稳定。
- 当前三轮都不符合真实生产流量模型，只能证明“47 个帖子均匀随机 + 热缓存”场景下接口稳定。

后续动作：

- 若要更贴近真实访问，建议增加热点倾斜模式，例如 80% 流量打少量 HOT_POSTS，20% 流量打长尾 POST_IDS。
- 继续压测同一场景时，建议把 `SUMMARY_PATH` 改成带轮次的文件名，避免覆盖上一轮 JSON。

## RUN-006：二八热点流量 - 学生帖子详情无预热

时间：2026-05-27 15:34 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-8020-$i \
SUMMARY_PATH=reports/k6/detail-8020-$i.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `page_size=20`
- 本轮未预热，让缓存随二八流量自然变热。
- 本轮未显式设置 `HOT_POST_IDS`，热点帖子不是服务端已知配置，只是压测脚本为了模拟“20% 帖子吸引 80% 流量”而构造的访问概率。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.2s`
- 完成 `7134` 次请求，`0` 次中断
- 吞吐约 `236.36 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 7134 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 126.19 ms |
| latency_p50 | 103.52 ms |
| latency_p90 | 231.06 ms |
| latency_p95 | 254.53 ms |
| latency_p99 | 374.49 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-8020--student-post-detail-vu4-0` | `a4d61f8ae17b96883e8c1c667133c2f6` | 200 | 515.749 ms |
| `detail-8020--student-post-detail-vu18-1` | `7ba88f57f61cda315150336b5997bc76` | 200 | 1325.127 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看无预热二八流量下的冷启动和自然热缓存过程。
- 第二条 trace 样例耗时 `1325.127ms`，但整体 p99 为 `374.49ms`，说明该样例属于极少数早期慢请求，建议在 Jaeger 单独查看是否发生缓存未命中或批量回源。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 二八热点无预热压测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=254.53ms < 1500ms`，`p99=374.49ms < 3000ms`，明显低于阈值。
- 吞吐 `236.36 req/s` 高于前三轮均匀随机测试，符合二八流量下少量 key 被快速打热、Redis 命中率更高的预期。
- 本轮比均匀随机更接近真实访问形态，但仍是合成流量模型；真实生产热点会随时间变化，热点集合不是服务端提前知道的。

后续动作：

- 后续同类测试建议把 `REQUEST_ID_PREFIX` 和 `SUMMARY_PATH` 改为固定轮次，例如 `detail-8020-r1`，不要在单次命令里使用未定义的 `$i`。
- 可以继续跑 `detail-8020-r2/r3`，观察二八模型下 p95/p99 是否稳定。

## RUN-007：二八热点流量 - hash 合成热点池

时间：2026-05-27 15:39 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-8020 \
SUMMARY_PATH=reports/k6/detail-8020.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `hot_pick=hash`
- `page_size=20`
- 本轮未显式设置 `HOT_POST_IDS`，脚本使用默认 `HOT_POST_PICK=hash` 从帖子池中稳定抽取合成热点。
- 本轮不预热，让缓存随二八流量自然变热。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.2s`
- 完成 `5056` 次请求，`0` 次中断
- 吞吐约 `167.35 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 5056 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 178.24 ms |
| latency_p50 | 113.93 ms |
| latency_p90 | 279.55 ms |
| latency_p95 | 298.15 ms |
| latency_p99 | 540.78 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-8020-student-post-detail-vu27-1` | `a80df654e3f4b4695ce59800b6968953` | 200 | 1242.146 ms |
| `detail-8020-student-post-detail-vu4-0` | `939fee68a91ba750f94b2c62d8dbfd47` | 200 | 1398.182 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看 hash 合成热点池下的早期慢请求。
- 两条 trace 样例都超过 `1.2s`，但整体 p99 为 `540.78ms`，说明采样命中了少数慢请求；建议在 Jaeger 里看是否集中在缓存未命中、评论列表回源或回复批量查询。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 二八热点 hash 模型压测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=298.15ms < 1500ms`，`p99=540.78ms < 3000ms`，明显低于阈值。
- 与 RUN-006 相比，本轮吞吐从 `236.36 req/s` 降至 `167.35 req/s`，p95/p99 上升；由于 RUN-006 未输出 `hot_pick`，且本轮已切换为 hash 合成热点池，两轮不作为严格 A/B，只作为二八模型不同热点集合下的结果参考。
- 当前结果仍说明二八流量下接口稳定，但需要继续跑 `detail-8020-r2/r3` 验证 hash 热点池下的稳定性。

后续动作：

- 建议继续用同一参数跑第二、第三轮：`SUMMARY_PATH=reports/k6/detail-8020-r2.json`、`reports/k6/detail-8020-r3.json`。
- 下一轮如果慢 trace 仍在 `1s+`，优先在 Jaeger 查看 `redis.GET student:post_comments`、`redis.MGET mysql:comment`、回复批量查询和 MySQL 回源 span。

## RUN-008：二八热点流量 - hash 合成热点池第二轮

时间：2026-05-27 15:41 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-8020 \
SUMMARY_PATH=reports/k6/detail-8020.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `hot_pick=hash`
- `page_size=20`
- 本轮未显式设置 `HOT_POST_IDS`，脚本继续使用默认 `HOT_POST_PICK=hash` 从帖子池中稳定抽取合成热点。
- 本轮不预热，让缓存随二八流量自然变热。

并发参数：

- `VUS=30`
- `DURATION=30s`
- 实际运行约 `30.2s`
- 完成 `4916` 次请求，`0` 次中断
- 吞吐约 `162.73 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 4916 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 183.42 ms |
| latency_p50 | 119.67 ms |
| latency_p90 | 290.26 ms |
| latency_p95 | 310.99 ms |
| latency_p99 | 531.03 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-8020-student-post-detail-vu30-1` | `4d471398f91a307c7f4aea22524c0100` | 200 | 517.795 ms |
| `detail-8020-student-post-detail-vu2-0` | `7fea6d1b0ea455c53c9662499c6c16bb` | 200 | 564.111 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看 hash 合成热点池下的早期请求。
- 本轮 trace 样例低于 RUN-007 的 `1s+` 样例，但整体 p95 比 RUN-007 略高，后续仍需看 Jaeger 里的缓存命中和评论/回复组装耗时。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- 二八热点 hash 第二轮通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=310.99ms < 1500ms`，`p99=531.03ms < 3000ms`，明显低于阈值。
- 与 RUN-007 相比，吞吐从 `167.35 req/s` 降至 `162.73 req/s`，p95 从 `298.15ms` 升至 `310.99ms`，p99 从 `540.78ms` 降至 `531.03ms`，整体波动可接受。
- 与 RUN-003 到 RUN-005 的均匀随机相比，hash 二八模型 p95 略高，但 p99 反而低于随机池的多轮 p99；当前不能简单判断“热点池一定更慢”，更可能是 hash 选出的 10 个热点帖子本身比平均帖子更重。

后续动作：

- 建议继续跑 `detail-8020-r3.json`，固定 hash 热点池后看三轮均值。
- 如需解释 hash 热点池为何偏慢，应统计这 10 个热点帖子的评论数、回复数、返回体大小和缓存命中情况，再和 47 个帖子池整体均值对比。

## RUN-009：二八热点流量 - 绕过缓存

时间：2026-05-27 15:45 Asia/Shanghai

场景：`student-post-detail`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
CACHE_MODE=bypass \
PAGE_SIZE=20 \
VUS=10 \
DURATION=30s \
MAX_P95_MS=2000 \
MAX_P99_MS=4000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-bypass \
SUMMARY_PATH=reports/k6/detail-bypass.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`，表示帖子池数量
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `hot_pick=hash`
- `cache_mode=bypass`
- `page_size=20`
- `CACHE_MODE=bypass` 会通过请求头 `x-cache-mode: bypass` 让后端跳过 Redis 缓存读写，适合观察 MySQL 主链路。

并发参数：

- `VUS=10`
- `DURATION=30s`
- 实际运行约 `30.3s`
- 完成 `1284` 次请求，`0` 次中断
- 吞吐约 `42.36 req/s`

缓存模式：`bypass`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 1284 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 234.00 ms |
| latency_p50 | 108.62 ms |
| latency_p90 | 493.51 ms |
| latency_p95 | 558.25 ms |
| latency_p99 | 642.55 ms |
| 阈值 | `p95 < 2000ms`、`p99 < 4000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `detail-bypass-student-post-detail-vu3-0` | `57c95f776eecd3d8925229305b2cf9c3` | 200 | 96.027 ms |
| `detail-bypass-student-post-detail-vu10-1` | `4ac6c69d6ed9a540f5848bffb95721a7` | 200 | 570.534 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看绕过 Redis 后的帖子详情主链路。
- 后续重点看 MySQL 查询 span：帖子查询、评论分页查询、评论批量查询和回复批量查询。

SQL 反查：

- 本轮未做 SQL 反查。

问题判断：

- bypass 模式 10 VU 压测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=558.25ms < 2000ms`，`p99=642.55ms < 4000ms`，满足本轮阈值。
- 与 RUN-007/RUN-008 的 30 VU 默认缓存模式相比，本轮并发更低但 p95/p99 更高，符合绕过 Redis 后更多请求进入 MySQL 主链路的预期。
- 本轮吞吐约 `42.36 req/s`，不能直接和 30 VU default 模式的吞吐对比；它主要用于回答“不靠 Redis 时主链路是否可用、延迟大约在哪个量级”。

后续动作：

- 如果要继续探 DB 上限，建议按 `10 VU -> 15 VU -> 20 VU` 阶梯提升，不要直接上 30 VU。
- 每轮都保留独立 summary，例如 `detail-bypass-v10.json`、`detail-bypass-v15.json`，方便横向比较。

## RUN-010：帖子搜索随机关键词第一轮

时间：2026-05-27 16:09 Asia/Shanghai

场景：`student-post-search`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-search \
KEYWORDS="$SEARCH_KEYWORDS" \
KEYWORD_MODE=random \
PAGE_SIZE=20 \
VUS=20 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=search-post-1 \
SUMMARY_PATH=reports/k6/search-post-1.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `keywords=14`
- `keyword_mode=random`
- `page_size=20`
- 未显式设置 `PAGE_NUM`，本轮按脚本默认值请求第 `1` 页。
- summary 中的 `post_ids=47`、`post_id_mode=random` 是脚本统一输出字段；`student-post-search` 实际按 `KEYWORDS` 选择关键词，不按 `post_id` 选择帖子。

并发参数：

- `VUS=20`
- `DURATION=30s`
- 实际运行约 `30.1s`
- 完成 `10987` 次请求，`0` 次中断
- 吞吐约 `365.57 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 10987 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 54.49 ms |
| latency_p50 | 52.07 ms |
| latency_p90 | 61.92 ms |
| latency_p95 | 65.12 ms |
| latency_p99 | 88.63 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `search-post-1-student-post-search-vu9-0` | `460e8bf9fb7ac33d2a0d5cd879611f51` | 200 | 165.806 ms |
| `search-post-1-student-post-search-vu7-1` | `01d197c988259f9ef5173f3679f80df9` | 200 | 171.57 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看搜索链路。
- 后续重点看 `redis.GET es:post_search` 是否命中；未命中时再看 ES 查询 span。

SQL 反查：

- 本轮帖子搜索走 ES 搜索链路，当前记录未做 SQL 反查。

问题判断：

- 本轮帖子搜索随机关键词压测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=65.12ms < 1500ms`，`p99=88.63ms < 3000ms`，明显低于阈值。
- 吞吐较高是合理现象：帖子搜索链路比帖子详情聚合链路轻，只有 14 个关键词会很快形成少量搜索缓存 key，且本轮未设置 `THINK_TIME`，20 个 VU 会持续紧密循环请求。
- 本轮结果代表“随机关键词 + 默认缓存”的帖子搜索表现，不代表帖子详情接口性能。

后续动作：

- 与 RUN-011 第二轮对比，观察搜索缓存进一步变热后的 p99 是否继续下降。
- 如需看 ES 本身成本，可补一轮 `CACHE_MODE=bypass` 的帖子搜索基准。

## RUN-011：帖子搜索随机关键词第二轮

时间：2026-05-27 16:10 Asia/Shanghai

场景：`student-post-search`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-search \
KEYWORDS="$SEARCH_KEYWORDS" \
KEYWORD_MODE=random \
PAGE_SIZE=20 \
VUS=20 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=search-post-2 \
SUMMARY_PATH=reports/k6/search-post-2.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `keywords=14`
- `keyword_mode=random`
- `page_size=20`
- 未显式设置 `PAGE_NUM`，本轮按脚本默认值请求第 `1` 页。
- summary 中的 `post_ids=47`、`post_id_mode=random` 是脚本统一输出字段；`student-post-search` 实际按 `KEYWORDS` 选择关键词，不按 `post_id` 选择帖子。

并发参数：

- `VUS=20`
- `DURATION=30s`
- 实际运行约 `30.0s`
- 完成 `11034` 次请求，`0` 次中断
- 吞吐约 `367.32 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 11034 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 54.26 ms |
| latency_p50 | 52.38 ms |
| latency_p90 | 61.91 ms |
| latency_p95 | 65.09 ms |
| latency_p99 | 74.14 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `search-post-2-student-post-search-vu6-1` | `1536cc1ce2d9ead0fcb438f737c2ed89` | 200 | 50.037 ms |
| `search-post-2-student-post-search-vu11-0` | `cd6b8cdb24ae201268d8be25596750c2` | 200 | 50.13 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看第二轮搜索链路。
- 第二轮 trace 样例都在 `50ms` 左右，建议在 Jaeger 中确认是否主要命中 `es:post_search` 搜索缓存。

SQL 反查：

- 本轮帖子搜索走 ES 搜索链路，当前记录未做 SQL 反查。

问题判断：

- 第二轮帖子搜索随机关键词压测通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=65.09ms < 1500ms`，`p99=74.14ms < 3000ms`，明显低于阈值。
- 与 RUN-010 相比，requests 从 `10987` 到 `11034`，吞吐从 `365.57 req/s` 到 `367.32 req/s`，p95 基本持平，p99 从 `88.63ms` 降至 `74.14ms`。
- 两轮结果说明 `student-post-search` 在 14 个随机关键词、默认缓存、20 VU 下非常稳定；高吞吐主要来自轻量搜索链路、缓存 key 数少以及无思考时间。

后续动作：

- 如果要模拟更真实的搜索行为，建议增加 `THINK_TIME`，并扩大关键词池或加入分页访问。
- 如果要证明缓存收益，建议增加一轮 `CACHE_MODE=bypass`，再和 RUN-010/RUN-011 横向比较。

## RUN-012：评论搜索二八帖子池第一轮

时间：2026-05-27 Asia/Shanghai

场景：`student-comment-search`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-comment-search \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
KEYWORDS="$COMMENT_SEARCH_KEYWORDS" \
KEYWORD_MODE=random \
PAGE_SIZE=20 \
VUS=20 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=search-comment-8020-1 \
SUMMARY_PATH=reports/k6/search-comment-8020-1.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `hot_pick=hash`
- `keywords=13`
- `keyword_mode=random`
- `page_size=20`
- 未显式设置 `PAGE_NUM`，本轮按脚本默认值请求第 `1` 页。

并发参数：

- `VUS=20`
- `DURATION=30s`
- 实际运行约 `30.1s`
- 完成 `9933` 次请求，`0` 次中断
- 吞吐约 `330.00 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 9933 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 60.28 ms |
| latency_p50 | 51.82 ms |
| latency_p90 | 62.70 ms |
| latency_p95 | 150.12 ms |
| latency_p99 | 182.15 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `search-comment-8020-1-student-comment-search-vu13-0` | `fd45d6edbcbc74a773727fe7b68b179e` | 200 | 191.559 ms |
| `search-comment-8020-1-student-comment-search-vu5-1` | `6f6feb075f2c883145a18a7515f81dd7` | 200 | 309.793 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看评论搜索链路。
- 后续重点看 `redis.GET es:comment_search` 是否命中；未命中时再看 ES 查询 span。

SQL 反查：

- 本轮评论搜索走 ES 搜索链路，当前记录未做 SQL 反查。

问题判断：

- 评论搜索二八帖子池第一轮通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=150.12ms < 1500ms`，`p99=182.15ms < 3000ms`，明显低于阈值。
- 第一轮 p95/p99 高于帖子搜索第二轮，符合预期：评论搜索 key 由 `keyword + post_id + page_num + page_size` 等条件共同决定，13 个关键词叠加 47 个帖子会产生较多搜索缓存组合，第一轮会有更多 ES 查询和缓存回填。
- 本轮没有设置 `THINK_TIME`，20 VU 持续请求，所以吞吐偏高。

后续动作：

- 与 RUN-013 第二轮对比，观察 `es:comment_search` 缓存变热后的延迟下降情况。
- 如需证明 ES 本身成本，可补一轮 `CACHE_MODE=bypass` 评论搜索基准。

## RUN-013：评论搜索二八帖子池第二轮

时间：2026-05-27 Asia/Shanghai

场景：`student-comment-search`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-comment-search \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
KEYWORDS="$COMMENT_SEARCH_KEYWORDS" \
KEYWORD_MODE=random \
PAGE_SIZE=20 \
VUS=20 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=search-comment-8020-2 \
SUMMARY_PATH=reports/k6/search-comment-8020-2.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=47`
- `post_id_mode=hotspot_80_20`
- `hot_posts=10`
- `tail_posts=37`
- `hot_traffic=80%`
- `hot_pick=hash`
- `keywords=13`
- `keyword_mode=random`
- `page_size=20`
- 未显式设置 `PAGE_NUM`，本轮按脚本默认值请求第 `1` 页。

并发参数：

- `VUS=20`
- `DURATION=30s`
- 实际运行约 `30.0s`
- 完成 `10669` 次请求，`0` 次中断
- 吞吐约 `355.63 req/s`

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 10669 |
| http_failed | 0.00% |
| checks_rate | 100.00% |
| latency_avg | 56.09 ms |
| latency_p50 | 54.20 ms |
| latency_p90 | 63.19 ms |
| latency_p95 | 67.59 ms |
| latency_p99 | 82.08 ms |
| 阈值 | `p95 < 1500ms`、`p99 < 3000ms` 均通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `search-comment-8020-2-student-comment-search-vu7-1` | `8efe6048e943b12b575e4d5d0ea1747e` | 200 | 50.055 ms |
| `search-comment-8020-2-student-comment-search-vu17-0` | `d93ad4fede1affad7a2e24657693f0a5` | 200 | 50.197 ms |

Jaeger 观察：

- 控制台已返回 trace_id，可用于查看第二轮评论搜索链路。
- 第二轮 trace 样例都在 `50ms` 左右，建议在 Jaeger 中确认是否主要命中 `es:comment_search` 搜索缓存。

SQL 反查：

- 本轮评论搜索走 ES 搜索链路，当前记录未做 SQL 反查。

问题判断：

- 评论搜索二八帖子池第二轮通过，`http_failed=0.00%`，`checks_rate=100.00%`。
- `p95=67.59ms < 1500ms`，`p99=82.08ms < 3000ms`，明显低于阈值。
- 与 RUN-012 相比，requests 从 `9933` 增至 `10669`，吞吐从约 `330.00 req/s` 增至约 `355.63 req/s`，p95 从 `150.12ms` 降至 `67.59ms`，p99 从 `182.15ms` 降至 `82.08ms`。
- 第二轮延迟明显下降，说明随机评论关键词 + 二八帖子池下，搜索缓存变热后收益很明显。

后续动作：

- 搜索缓存效果已经比较清楚；后续可补一轮 `CACHE_MODE=bypass`，单独评估 ES 查询在相同 13 个关键词和 47 个帖子组合下的成本。
- 如果要更贴近用户行为，可以加 `THINK_TIME=0.3` 或扩大评论关键词池，避免压测过度集中在少量 key 上。

## RUN-014：单热点帖子并发点赞故障

时间：2026-05-28 Asia/Shanghai

场景：`like-post`

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=like-post \
POST_ID=$HOT_POST_ID \
POST_IDS=$HOT_POST_ID \
POST_ID_MODE=round_robin \
STUDENT_START=$STUDENT_START \
VUS=30 \
DURATION=20s \
ALLOW_BUSINESS_ERRORS=true \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
REQUEST_ID_PREFIX=like-many \
SUMMARY_PATH=reports/k6/like-many.json \
k6 run tools/k6/comment-service.js
```

测试数据：

- `base_url=http://127.0.0.1:8888`
- `post_ids=1`
- `post_id_mode=round_robin`
- 实际热点帖子：`post_id=16966883282522112`
- 30 个 VU 同时给同一个帖子点赞，属于单行热点写压测。

并发参数：

- `VUS=30`
- `DURATION=20s`
- 实际运行约 `21.5s`
- 完成 `398` 次请求，`0` 次中断

缓存模式：`default`

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 398 |
| http_failed | 8.79% |
| checks_rate | 95.60% |
| latency_avg | 1563.48 ms |
| latency_p50 | 1522.94 ms |
| latency_p90 | 2878.27 ms |
| latency_p95 | 3058.09 ms |
| latency_p99 | 3059.72 ms |
| 阈值 | `p95 < 1500ms` 未通过，`p99 < 3000ms` 未通过 |

trace_id 样例：

| request_id | trace_id | status | duration |
| --- | --- | --- | --- |
| `like-many-like-post-vu30-0` | `064bf6a306ecb39a52c46176f9a77117` | 200 | 1623.446 ms |

错误样例：

| request_id | status | 错误 |
| --- | --- | --- |
| `like-many-like-post-vu10-2` | 500 | `context deadline exceeded` |
| `like-many-like-post-vu11-121` | 500 | `Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction` |

Jaeger 观察：

- 控制台已返回 trace_id，可按 `064bf6a306ecb39a52c46176f9a77117` 追踪单次请求。
- 失败集中发生在热点写窗口，p95/p99 接近服务端 `3s` 超时，说明请求主要卡在 MySQL 热点写锁等待。

SQL 反查：

- 本轮主要依据接口错误、k6 summary 和 MySQL 错误码判断。
- 后续复测时需要补充：

```sql
SELECT p.post_id,
       p.like_count,
       COUNT(pl.id) AS active_like_count,
       p.like_count - COUNT(pl.id) AS diff
FROM post p
LEFT JOIN post_like pl
  ON pl.post_id = p.post_id AND pl.status = 1
WHERE p.post_id = 16966883282522112
GROUP BY p.post_id, p.like_count;
```

问题判断：

- 本轮失败不是 k6 脚本问题，而是单热点帖子点赞把所有请求集中到同一条 `post` 记录。
- 原实现每个真实新增点赞都会在同一个 MySQL 事务内写 `post_like`，再执行 `post.like_count = like_count + 1`。
- 30 VU 同时更新同一行 `post.like_count` 时，MySQL 行锁竞争明显，部分事务出现 `1213 deadlock`，部分请求等待超过服务端 `3s` deadline 后返回 500。
- `ALLOW_BUSINESS_ERRORS=true` 只允许 4xx 业务错误作为预期结果，500 仍然是服务端故障。

解决方案：

- 保留 `post_like` 作为点赞关系事实源，继续用唯一键保证同一学生同一帖子幂等。
- 移除请求路径中同步更新 `post.like_count` 的热点单行写。
- 状态真实变化后，将 `post_id -> delta` 写入 Redis：
  - `counter:post_like:delta`：Hash，按帖子聚合 `+1/-1` 变化量。
  - `queue:post_like:dirty`：Set，作为待刷库帖子队列。
  - `queue:post_like:dirty_at`：ZSet，记录最后一次变化时间，task 只刷超过安静窗口的帖子。
- 大量用户对同一帖子取消点赞时也走同一套异步链路：请求只写 `delta=-1`，不在 service 内同步扣减 `post.like_count`。
- `comment-service` 只有在 `SADD queue:post_like:dirty {post_id}` 返回 `1` 时才发送 Kafka dirty 通知，避免热点帖子每次点赞都产生重复消息。
- 点赞/取消点赞 dirty 通知使用独立 Kafka topic：`postlike`；原 `comment-service` topic 继续给 Canal 数据库变更同步使用，避免两类消息混在一起。
- `comment-task` 消费 Kafka dirty 通知后，从 Redis claim 聚合 delta，并用一条聚合 SQL 更新 MySQL：

```sql
UPDATE post
SET like_count = GREATEST(like_count + ?, 0)
WHERE post_id = ?;
```

- 读帖子详情时叠加 Redis 中尚未落库的 pending delta，避免异步落库窗口内展示旧点赞数。
- task 不再收到 Kafka dirty 就立刻更新 `post.like_count`；同一帖子持续有请求时先继续聚合，安静至少 5 秒后再刷库，避免和 `post_like` 外键/索引写入抢 `post` 行。
- task 刷库成功后删除 `mysql:post:{post_id}` 对象缓存；刷库失败时把 delta 重新写回 Redis 队列，等待下一轮重试。
- 如果 Kafka 发送失败或消息丢失，Redis dirty set 中仍保留 `post_id`，`comment-task` 会低频兜底扫描并补刷。

后续动作：

- 重启服务后复跑本轮 `like-post` 单热点 30 VU / 20s。
- 复测目标：`http_failed=0.00%`，无 `context deadline exceeded` / `1213 deadlock`，p95 低于 `1500ms`。
- 复测后用 SQL 反查 `post.like_count` 与 `post_like.status=1` 的聚合计数是否一致。

## RUN-015：同一学生重复取消点赞业务错误映射

日期：2026-05-28

场景：`unlike-post`

目标：

- 验证同一学生对同一帖子重复取消点赞时，不会重复扣减 `like_count`。
- 验证并发冲突应该返回可接受的 4xx 业务错误，而不是 5xx 服务端错误。

命令：

```bash
BASE_URL=$BASE_URL \
SCENARIO=unlike-post \
POST_ID=$HOT_POST_ID \
STUDENT_START=$STUDENT_START \
SAME_USER=true \
VUS=30 \
DURATION=10s \
ALLOW_BUSINESS_ERRORS=true \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
REQUEST_ID_PREFIX=unlike-same-user \
SUMMARY_PATH=reports/k6/unlike-same-user.json \
k6 run tools/k6/comment-service.js
```

k6 summary：

| 指标 | 结果 |
| --- | --- |
| requests | 728 |
| http_failed | 27.20% |
| checks_rate | 86.40% |
| latency_avg | 422.72 ms |
| latency_p50 | 358.03 ms |
| latency_p90 | 608.82 ms |
| latency_p95 | 890.46 ms |
| latency_p99 | 1517.72 ms |
| 阈值 | `p95 < 1500ms` 通过，`p99 < 3000ms` 通过，`checks > 95%` 未通过 |

问题判断：

- 本轮延迟指标没有触发超时，但 `ALLOW_BUSINESS_ERRORS=true` 后仍有 198 次 `status is expected` 检查失败。
- k6 脚本已允许 4xx 作为重复取消点赞的业务结果，因此这些失败说明服务把部分并发冲突映射成了 5xx。
- 代码定位后，主要有两类非业务化错误：
  - 同一学生同一帖子并发 DELETE 争抢 Redis 短锁，拿不到锁时返回普通 `error`，Kratos 会按未知错误映射成 500。
  - 已经取消过或本来未点赞时，旧代码把 `RowsAffected=0` 当成 200 幂等成功，压测结果无法区分“真正取消成功”和“重复取消”。

修改方案：

- 在 biz 层定义学生点赞业务哨兵错误：
  - `ErrPostLikeBusy`：同一学生同一帖子操作过于频繁。
  - `ErrPostNotLiked`：当前没有有效点赞关系，属于重复取消或未点赞取消。
- data 层 `UnlikePost` 中，只有 `status=1 -> 0` 且 `RowsAffected=1` 才写入 `like_count=-1` delta。
- data 层 `UnlikePost` 中，`RowsAffected=0` 返回 `ErrPostNotLiked`，不删除缓存、不写 Redis delta、不扣计数。
- service 层统一映射业务错误：
  - `ErrPostNotLiked` -> HTTP `409`，reason=`POST_NOT_LIKED`。
  - `ErrPostLikeBusy` -> HTTP `429`，reason=`POST_LIKE_BUSY`。
- `LikePost` 也复用 `ErrPostLikeBusy -> 429`，避免同类短锁冲突继续冒泡成 500。

复测目标：

- `status is expected` 通过率恢复到 95% 以上。
- 重复取消请求允许出现 409/429，但不允许出现 500。
- SQL 反查 `post.like_count >= 0`，并与 `post_like.status=1` 聚合计数保持一致。

## RUN-016：单热点帖子大量用户取消点赞异步落库方案

日期：2026-05-28

场景：`unlike-post`

目标：

- 验证大量不同学生对同一帖子取消点赞时，请求链路不再同步更新同一行 `post.like_count`。
- 验证取消点赞的 `-1` 计数变化与点赞的 `+1` 变化一样，由 Redis 聚合、Kafka 通知 `comment-task` 异步落库。

问题判断：

- 单热点帖子取消点赞和单热点帖子点赞本质上会争抢同一条 `post` 记录的计数字段。
- 如果每个取消点赞请求都同步执行 `post.like_count = like_count - 1`，大量用户同时取消同一帖子点赞时仍会出现热点行锁竞争。
- 重复取消只是不应该扣减计数；真正有效的 `status=1 -> 0` 取消动作也不能在 service 内同步扣 `post.like_count`。

修改方案：

- `comment-service` 中 `UnlikePost` 只同步更新点赞关系表 `post_like.status`。
- 只有 `RowsAffected=1` 的真实取消动作，才调用 `recordPostLikeCountDelta(post_id, -1)`。
- `recordPostLikeCountDelta` 只写 Redis：
  - `HINCRBY counter:post_like:delta {post_id} -1`
  - `SADD queue:post_like:dirty {post_id}`
  - `ZADD queue:post_like:dirty_at {now_ms} {post_id}`
- 只有 `SADD` 返回 `1` 时发送 Kafka dirty 通知；返回 `0` 表示该帖子已经处于 dirty 状态，不重复发消息。
- `comment-task` 消费 Kafka dirty 通知后统一领取 delta，并执行：

```sql
UPDATE post
SET like_count = GREATEST(like_count + ?, 0)
WHERE post_id = ?;
```

- 这样大量取消点赞会被 Redis 聚合成少量 DB 更新，Kafka 只负责唤醒 task，`GREATEST` 兜底避免异常情况下计数扣成负数。

复测目标：

- 单热点大量取消点赞下不出现 `context deadline exceeded` 或 MySQL `1213/1205` 热点计数字段冲突。
- 接口允许重复取消返回 409、短锁忙返回 429，但不允许 500。
- task 刷库后 SQL 反查 `post.like_count` 与 `post_like.status=1` 聚合计数一致。

## RUN-017：单热点点赞异步落库二次优化

日期：2026-05-28

场景：`like-post`

本轮结果：

| 指标 | 结果 |
| --- | --- |
| requests | 752 |
| http_failed | 3.99% |
| checks_rate | 98.07% |
| latency_avg | 809.84 ms |
| latency_p50 | 618.41 ms |
| latency_p90 | 970.95 ms |
| latency_p95 | 2328.84 ms |
| latency_p99 | 3204.96 ms |

问题判断：

- service 已经不再同步更新 `post.like_count`，但 `post_like` 关系表写入仍然是热点写。
- 原点赞逻辑是“更新历史取消记录 -> 查询已点赞 -> 插入”，多语句事务在同一 `post_id` 高并发下仍可能触发 InnoDB gap/next-key lock 死锁。
- 如果 `post_like.post_id` 有外键指向 `post`，插入 `post_like` 会持有父表 `post` 行共享锁；task 立刻更新 `post.like_count` 会抢同一行排他锁，仍可能出现 `1213 deadlock` 或请求等待到 3 秒超时。
- Redis 是远程实例，本轮还出现了一次 `i/o timeout`；计数入队不能继续把点赞关系写成功的请求拖成 500。

二次修改方案：

- `LikePost` 改成单条 upsert：新增、恢复取消、已点赞幂等都由一条 SQL 完成，减少锁范围。
- `UnlikePost` 改成单条 update：只有 `status=1 -> 0` 才写 `delta=-1`，重复取消仍返回 409。
- MySQL `1213/1205/1062` 重试耗尽后映射为 `ErrPostLikeBusy -> HTTP 429`，避免热点瞬时冲突冒泡成 500。
- Redis delta 入队增加短超时；失败时记录日志但不让关系请求返回 500，`post_like` 仍作为事实源。
- Redis 新增 `queue:post_like:dirty_at`，每次 `HINCRBY` 都刷新最后变化时间。
- `comment-task` 只处理超过 5 秒安静窗口的帖子；Kafka dirty 消息只负责唤醒，真正 claim 前会检查 `dirty_at`。

复测关注：

- `http_failed` 应降到 0 或只剩允许的 4xx 业务错误。
- 不应再看到 `post.like_count` 热点行导致的 `1213 deadlock`。
- 压测结束后等待 5 秒以上，再校验 `post.like_count` 与 `post_like.status=1` 聚合值是否一致。
