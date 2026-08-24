# comment-service 压测方案

## 1. 压测目标

这次压测不是只看 QPS，而是回答四个问题：

| 目标 | 要证明什么 | 主要证据 |
| --- | --- | --- |
| 读链路性能 | 帖子详情、评论列表在缓存命中、多 key、绕过缓存时是否稳定 | k6 summary + Jaeger |
| 缓存设计有效性 | 写路径只删缓存，读路径回源后再加载缓存是否符合预期 | Redis key + Jaeger span |
| 搜索性能 | ES 查询和搜索缓存是否稳定 | k6 summary + ES/Jaeger |
| 并发写正确性 | 点赞、取消点赞、评论、审核是否幂等，计数是否正确 | k6 summary + SQL 反查 |

## 2. 压测前准备

启动服务和 Jaeger：

```bash
docker run --rm --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

```bash
TRACE_ENABLED=true \
OTEL_SERVICE_NAME=comment-service \
OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4317 \
TRACE_SAMPLE_RATIO=1 \
go run ./cmd/comment-service -conf ./configs
```

创建结果目录：

```bash
mkdir -p reports/k6
```

Jaeger 地址：

```text
http://127.0.0.1:16686
```

## 3. 测试数据

先准备这些变量：

```bash
export BASE_URL=http://127.0.0.1:8888
export HOT_POST_ID=16966883282522112
export COMMENT_ID=2832947245748224
export PENDING_COMMENT_ID=2832947245748224
export OPERATOR_ID=1
export STUDENT_START=200000
```

直接使用下面这批测试数据：

```bash
export POST_IDS="2799686486331392,2799925473579008,2800952469884928,2801496957652992,2801999431077888,2802100568330240,2802835519442944,2803298511884288,4227151725334528,4230251047555072,4230746348720128,4282890007351296,6836025619910656,6836808826490880,6838462665723904,6840268313595904,6840353931923456,6841438692184064,6841635065303040,6841929673216000,6842239900717056,6842624161878016,7550979079671808,7552534386315264,7552586538291200,7559510809907200,9974924949065728,9976171206807552,9976973287755776,9977716677808128,10020749980995584,10021455404208128,10021714633166848,10430840790061056,10431254432321536,10455827219484672,15124419483537408,16907453014740992,16920404727697408,16962946500399104,16966883282522112,16969749925728256,16970044122599424,16971234382188544,16977783062269952,17291524748349440,17295384497885184"

export COMMENT_IDS="2832947245748224,2833899122069504,2834401465470976,2834520982163456,2835061174964224,6836026483937280,6836809648574464,6838463492001792,6840269156651008,6840354754007040,6841439535239168,6841635883192320,6841930482716672,6842240706023424,6842624992350208,7560137568948224,7561963974430720,7562033151086592,7562037806764032,7562873052073984,9948924169162752,9952120295919616,9979629070716928,10003714429423616,10004753614376960,10005050965364736,10024480034263040,10024570148884480,10431724181786624,10432068672557056,10435058049486848,10455828251283456,16963037655207936,16963041941786624,16963046333222912,16963050003238912,16963053719392256,16963056944812032,16970122941960192,16971271946375168,16977815559737344,17275747815133184,17275886868893696,17286464186355712,17286558902128640,17291387745603584,17291443177525248,17291638548205568,17295536923086848"

export COMMENT_REPLY_IDS="6836028140687360,6836811305324544,6838301617033216,6838465127780352,6840069411311616,6840270830178304,6840356406562816,6841441191989248,6841637544136704,6841932105912320,6842242358579200,6842627697676288,7563068250787840,7579165461057536,10455830495236096,16963171327676416,16963176675414016,16963181528223744,16963186053877760"
```

数据量：

```bash
echo "POST_IDS count=$(echo "$POST_IDS" | tr ',' '\n' | wc -l | tr -d ' ')"
echo "COMMENT_IDS count=$(echo "$COMMENT_IDS" | tr ',' '\n' | wc -l | tr -d ' ')"
echo "COMMENT_REPLY_IDS count=$(echo "$COMMENT_REPLY_IDS" | tr ',' '\n' | wc -l | tr -d ' ')"
```

如果后面你手里还有一列 ID，可以这样转成逗号分隔：

```bash
pbpaste | tr '\n' ',' | sed 's/,$//'
```

检查 `POST_IDS` 是否都可访问：

```bash
for id in $(echo "$POST_IDS" | tr ',' ' '); do
  code=$(curl -s -o /tmp/comment_service_post_check_body -w "%{http_code}" \
    "$BASE_URL/v1/student/posts/$id?comment_page_num=1&comment_page_size=20")
  if [ "$code" != "200" ]; then
    echo "BAD_POST_ID id=$id status=$code body=$(cat /tmp/comment_service_post_check_body)"
  fi
done
```

检查 `COMMENT_IDS` 是否都可访问时，可以先用第一个评论 ID 做 Smoke；当前 k6 脚本只支持单个 `COMMENT_ID`，不支持评论 ID 池：

```bash
export COMMENT_ID=$(echo "$COMMENT_IDS" | cut -d',' -f1)
```

## 4. k6 通用参数

统一脚本：

```text
tools/k6/comment-service.js
```

常用参数：

| 参数 | 含义 |
| --- | --- |
| `SCENARIO` | 场景名，例如 `student-post-detail` |
| `POST_ID` | 单个帖子 ID |
| `POST_IDS` | 多帖子 ID 池 |
| `POST_ID_MODE` | `round_robin`、`random` 或 `hotspot_80_20` |
| `HOT_TRAFFIC_PERCENT` | 热点流量比例，默认 `80`，仅 `hotspot_80_20` 使用 |
| `HOT_POST_PERCENT` | 热点帖子占帖子池比例，默认 `20`，仅 `hotspot_80_20` 使用 |
| `HOT_POST_IDS` / `TAIL_POST_IDS` | 手动指定热点帖子池和长尾帖子池；不设置时由脚本按 `HOT_POST_PERCENT` 自动构造模拟热点池 |
| `HOT_POST_PICK` | 自动构造热点池的方式，默认 `hash`，可设为 `first` |
| `HOT_POST_SEED` | `HOT_POST_PICK=hash` 时的稳定种子，默认 `comment-service-8020` |
| `KEYWORDS` | 搜索关键词池，英文逗号分隔 |
| `KEYWORD_MODE` | `round_robin` 或 `random` |
| `PAGE_NUM` / `PAGE_SIZE` | 分页参数 |
| `VUS` / `DURATION` | 固定并发压测 |
| `ITERATIONS` | 固定总请求次数，适合只创建 1 条测试数据 |
| `STAGES` | 阶梯压测，设置后覆盖 `VUS/DURATION` |
| `CACHE_MODE=bypass` | 请求级绕过缓存，用来看 DB 主链路 |
| `ALLOW_BUSINESS_ERRORS=true` | 写接口允许合理 4xx 并发冲突 |
| `LOG_RESPONSE=true` | 打印成功响应体，适合记录新增评论返回的 `comment_id` |
| `TRACE_SAMPLES_PER_VU` | 每个 VU 打印多少条 trace sample |
| `SUMMARY_PATH` | k6 JSON 输出文件 |

脚本当前支持这些场景：

| 场景 | `SCENARIO` |
| --- | --- |
| 学生帖子详情 | `student-post-detail` |
| 学生评论列表 | `student-comment-list` |
| 学生搜索帖子 | `student-post-search` |
| 学生搜索评论 | `student-comment-search` |
| 点赞 | `like-post` |
| 取消点赞 | `unlike-post` |
| 发表评论 | `create-comment` |
| 审核通过 | `audit-approve` |
| 审核驳回 | `audit-reject` |

## 5. 第一阶段：Smoke Test

目标：先确认服务、k6、trace 全部能跑通。

### 5.1 帖子详情

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

验收：

- `http_failed=0.00%`
- `checks_rate=100.00%`
- 控制台有 `trace_id`
- Jaeger 能看到完整链路

### 5.2 评论列表

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

重点看 Jaeger：

```text
redis.GET student:post_comments
redis.MGET mysql:comment
mysql.SELECT study_comment.list 或 batch_by_ids
```

## 6. 第二阶段：读链路

### 6.1 单热点热缓存

先预热：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_ID=$HOT_POST_ID \
PAGE_SIZE=20 \
VUS=1 \
DURATION=5s \
REQUEST_ID_PREFIX=warmup-detail-hot \
k6 run tools/k6/comment-service.js
```

正式跑：

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

结论用途：说明热点详情在 Redis 命中后的性能。

### 6.2 多帖子 round_robin

```bash
for i in 1 2 3; do
  BASE_URL=$BASE_URL \
  SCENARIO=student-post-detail \
  POST_IDS="$POST_IDS" \
  POST_ID_MODE=round_robin \
  PAGE_SIZE=20 \
  VUS=30 \
  DURATION=30s \
  MAX_P95_MS=1500 \
  MAX_P99_MS=3000 \
  TRACE_SAMPLES_PER_VU=2 \
  REQUEST_ID_PREFIX=detail-robin-$i \
  SUMMARY_PATH=reports/k6/detail-robin-$i.json \
  k6 run tools/k6/comment-service.js
done
```

结论用途：避免单个 key 过热，观察多帖子池下的读性能。

### 6.3 多帖子 random

```bash
for i in 1 2 3; do
  BASE_URL=$BASE_URL \
  SCENARIO=student-post-detail \
  POST_IDS="$POST_IDS" \
  POST_ID_MODE=random \
  PAGE_SIZE=20 \
  VUS=30 \
  DURATION=30s \
  MAX_P95_MS=1500 \
  MAX_P99_MS=3000 \
  TRACE_SAMPLES_PER_VU=2 \
  REQUEST_ID_PREFIX=detail-random-$i \
  SUMMARY_PATH=reports/k6/detail-random-$i.json \
  k6 run tools/k6/comment-service.js
done
```

结论用途：作为“均匀随机帖子池”的热缓存基准。它比 round_robin 更自然，但仍不是生产真实热点分布。

### 6.4 二八热点流量

目标：模拟“20% 的帖子吸引 80% 的访问”，比 `random` 更接近真实读流量。

设计：

- `POST_ID_MODE=hotspot_80_20`
- `HOT_TRAFFIC_PERCENT=80`：80% 请求打热点池
- `HOT_POST_PERCENT=20`：默认从 `POST_IDS` 中稳定抽取 20% 作为合成热点池
- 剩余 20% 请求打长尾池
- 当前 `POST_IDS=47` 时，默认热点池约 `10` 个帖子，长尾池约 `37` 个帖子
- 这里不预热，让缓存按二八流量自然变热，观察冷启动到热态过程中的整体表现
- 服务端不需要提前知道哪些帖子是热点；热点只体现在压测脚本发请求的概率分布上
- 如果不设置 `HOT_POST_IDS`，脚本用 `HOT_POST_PICK=hash` 和 `HOT_POST_SEED` 自动构造一组合成热点帖子，用来模拟真实用户访问自然形成的热点

单轮执行：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
PAGE_SIZE=20 \
VUS=30 \
DURATION=30s \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=2 \
REQUEST_ID_PREFIX=detail-8020-r1 \
SUMMARY_PATH=reports/k6/detail-8020-r1.json \
k6 run tools/k6/comment-service.js
```

三轮复测：

```bash
for i in 1 2 3; do
  BASE_URL=$BASE_URL \
  SCENARIO=student-post-detail \
  POST_IDS="$POST_IDS" \
  POST_ID_MODE=hotspot_80_20 \
  HOT_TRAFFIC_PERCENT=80 \
  HOT_POST_PERCENT=20 \
  HOT_POST_PICK=hash \
  HOT_POST_SEED=comment-service-8020 \
  PAGE_SIZE=20 \
  VUS=30 \
  DURATION=30s \
  MAX_P95_MS=1500 \
  MAX_P99_MS=3000 \
  TRACE_SAMPLES_PER_VU=2 \
  REQUEST_ID_PREFIX=detail-8020-$i \
  SUMMARY_PATH=reports/k6/detail-8020-$i.json \
  k6 run tools/k6/comment-service.js
done
```

结论用途：

- 对比 6.3 的 `random`，判断热点倾斜后 p95/p99 是否更高。
- 观察热点 key 是否因为集中访问出现抖动，例如帖子对象、评论列表、回复列表缓存过期后被集中回源。
- 如果 p99 明显上升，优先看 Jaeger 里的 `redis.GET mysql:post`、`redis.GET student:post_comments`、`redis.MGET mysql:comment` 和 `redis.MGET student:comment_replies`。
- 这组结果比 `random` 更适合作为“接近真实读流量”的面试和复盘依据。

### 6.5 绕过缓存看 DB 主链路

从 10 VU 开始，不要直接上高并发：

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

结论用途：回答“不靠 Redis，MySQL 主链路能扛多少”。

### 6.6 评论列表分页大小

```bash
for size in 20 50 100; do
  BASE_URL=$BASE_URL \
  SCENARIO=student-comment-list \
  POST_IDS="$POST_IDS" \
  POST_ID_MODE=hotspot_80_20 \
  HOT_TRAFFIC_PERCENT=80 \
  HOT_POST_PERCENT=20 \
  HOT_POST_PICK=hash \
  HOT_POST_SEED=comment-service-8020 \
  PAGE_SIZE=$size \
  VUS=30 \
  DURATION=30s \
  MAX_P95_MS=1500 \
  MAX_P99_MS=3000 \
  TRACE_SAMPLES_PER_VU=2 \
  REQUEST_ID_PREFIX=comment-list-8020-p$size \
  SUMMARY_PATH=reports/k6/comment-list-8020-p$size.json \
  k6 run tools/k6/comment-service.js
done
```

重点看：

- `page_size=100` 时 p95/p99 是否上升明显。
- Jaeger 里 `redis.MGET mysql:comment` 的 `cache.keys.count`。
- MySQL 评论分页查询是否变慢。

## 7. 第三阶段：缓存策略验证

这一阶段专门验证“写数据不加载缓存，读数据再加载缓存”。

缓存验证统一使用二八流量模型：

```text
POST_ID_MODE=hotspot_80_20
HOT_TRAFFIC_PERCENT=80
HOT_POST_PERCENT=20
HOT_POST_PICK=hash
HOT_POST_SEED=comment-service-8020
```

这样新增评论、读回填、列表查询都落在同一套合成热点分布下，不再混用单热点或均匀随机模型。

步骤：

1. 通过 `create-comment` 新增一条评论。
2. 先不要调用任何查询接口。
3. 用 Redis 查新评论对象 key。
4. 再调用评论详情或评论列表。
5. 再查 Redis，确认读路径已经回填。

新增评论：

```bash
BASE_URL=$BASE_URL \
SCENARIO=create-comment \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
STUDENT_START=$STUDENT_START \
VUS=1 \
ITERATIONS=1 \
DURATION=10s \
MAX_P95_MS=2000 \
MAX_P99_MS=4000 \
TRACE_SAMPLES_PER_VU=1 \
LOG_RESPONSE=true \
REQUEST_ID_PREFIX=cache-aside-8020-create-comment \
SUMMARY_PATH=reports/k6/cache-aside-8020-create-comment.json \
k6 run tools/k6/comment-service.js
```

验证点：

```text
新增后未查询：
GET mysql:comment:{new_comment_id} 应该为空

查询对应 post_id 的评论列表或评论详情后：
GET mysql:comment:{new_comment_id} 可以出现值
```

注意：`BF.ADD bf:comment` 会出现，这是 Bloom Filter，不是数据缓存。
注意：新增评论时要从响应里记下 `post_id` 和 `comment_id`，后续查 Redis 和 SQL 反查都以这两个 ID 为准。

## 8. 第四阶段：搜索接口

搜索接口使用随机关键词池，避免一直请求同一个搜索缓存 key。

帖子搜索和评论搜索使用不同关键词池，避免用帖子标题/正文关键词去测评论搜索，导致大量空结果：

```bash
export POST_SEARCH_KEYWORDS="雅思,雅思口语,雅思听力,听力8.0,提分攻略,学习帖,Postman,修改测试贴,nihao,干货内容,长难句,主谓宾,万事不要浮躁,认真做"

export COMMENT_SEARCH_KEYWORDS="老师,讲得太好,打卡学习,精听,听不懂,长难句,怎么办,已学,k6,load test comment,comment 0,comment 1,vu 1"
```

### 8.0 批量生成评论搜索语料

如果帖子池里的评论数量太少，先用下面脚本给每个帖子批量生成包含上述评论关键词的评论。

默认模板：

- `老师讲得太好啦，打卡学习！`
- `老师，精听遇到听不懂的长难句怎么办？`
- `已学`
- `k6 load test comment 0 from vu 1`
- `k6 load test comment 1 from vu 1`

先 dry-run 看预计生成量：

```bash
BASE_URL=$BASE_URL \
POST_IDS="$POST_IDS" \
STUDENT_START=$STUDENT_START \
REPEAT_PER_POST=4 \
CONCURRENCY=5 \
DRY_RUN=true \
node tools/seed-comments.js
```

确认无误后执行：

```bash
BASE_URL=$BASE_URL \
POST_IDS="$POST_IDS" \
STUDENT_START=$STUDENT_START \
REPEAT_PER_POST=4 \
CONCURRENCY=5 \
REQUEST_ID_PREFIX=seed-comment-keywords \
node tools/seed-comments.js
```

说明：

- `REPEAT_PER_POST=4` 表示每个帖子写入 `5 * 4 = 20` 条评论。
- 当前帖子池 `47` 个帖子时，总共会生成 `47 * 20 = 940` 条评论。
- 如需每条内容唯一，可以加 `UNIQUE_SUFFIX=true`，脚本会在评论末尾追加 `#seed-*` 后缀，但仍保留搜索关键词。
- 新评论进入 ES 依赖同步链路，生成后建议等一小段时间再执行搜索压测。

### 8.1 搜索帖子

```bash
for i in 1 2; do
  BASE_URL=$BASE_URL \
  SCENARIO=student-post-search \
  KEYWORDS="$POST_SEARCH_KEYWORDS" \
  KEYWORD_MODE=random \
  PAGE_SIZE=20 \
  VUS=20 \
  DURATION=30s \
  MAX_P95_MS=1500 \
  MAX_P99_MS=3000 \
  TRACE_SAMPLES_PER_VU=2 \
  REQUEST_ID_PREFIX=search-post-$i \
  SUMMARY_PATH=reports/k6/search-post-$i.json \
  k6 run tools/k6/comment-service.js
done
```

第一轮主要看 ES，第二轮看搜索缓存效果。

### 8.2 搜索评论

```bash
for i in 1 2; do
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
  REQUEST_ID_PREFIX=search-comment-8020-$i \
  SUMMARY_PATH=reports/k6/search-comment-8020-$i.json \
  k6 run tools/k6/comment-service.js
done
```

评论关键词来源：

- `老师讲得太好啦，打卡学习！`
- `老师，精听遇到听不懂的长难句怎么办？`
- `已学`
- `k6 load test comment 0 from vu 1`
- `k6 load test comment 1 from vu 1`

结论用途：第一轮主要看 ES 查询和搜索缓存回填，第二轮看 `es:comment_search` 搜索缓存命中后的延迟变化。

## 9. 第五阶段：并发写

写接口必须做 SQL 反查，不能只看 k6 summary。

点赞/取消点赞的 `post.like_count` 是异步落库：压测结束后至少等待 5 秒安静窗口，再执行 SQL 校验。请求期间可以先看 Redis pending delta，最终以 `post_like.status=1` 作为事实源。

### 9.1 多学生点赞同一帖子

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

反查：

```sql
SELECT p.post_id,
       p.like_count,
       COUNT(pl.id) AS active_like_count,
       p.like_count - COUNT(pl.id) AS diff
FROM post p
LEFT JOIN post_like pl
  ON pl.post_id = p.post_id AND pl.status = 1
WHERE p.post_id = ?
GROUP BY p.post_id, p.like_count;
```

验收：

- 不允许 5xx。
- 允许热点瞬时冲突映射为 429，但不应出现 `1213/1205` 直接冒泡。
- 等 task 刷库后 `diff=0`。

### 9.2 同一学生重复点赞

```bash
BASE_URL=$BASE_URL \
SCENARIO=like-post \
POST_ID=$HOT_POST_ID \
STUDENT_START=$STUDENT_START \
SAME_USER=true \
VUS=30 \
DURATION=10s \
ALLOW_BUSINESS_ERRORS=true \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
REQUEST_ID_PREFIX=like-same-user \
SUMMARY_PATH=reports/k6/like-same-user.json \
k6 run tools/k6/comment-service.js
```

验收：

- 允许合理 4xx。
- 不允许 5xx。
- 同一学生同一帖子只能有一条有效点赞。
- `like_count` 在 task 刷库后不能重复增加；请求期间允许 MySQL 计数短暂落后。

### 9.3 同一学生重复取消点赞

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

验收：

- 允许合理 4xx。
- 不允许 5xx。
- 重复取消返回 409 或短锁忙返回 429。
- `like_count` 在 task 刷库后不能扣成负数。

### 9.4 发表评论

```bash
BASE_URL=$BASE_URL \
SCENARIO=create-comment \
POST_ID=$HOT_POST_ID \
STUDENT_START=$STUDENT_START \
VUS=20 \
DURATION=20s \
MAX_P95_MS=2000 \
MAX_P99_MS=4000 \
REQUEST_ID_PREFIX=create-comment \
SUMMARY_PATH=reports/k6/create-comment.json \
k6 run tools/k6/comment-service.js
```

反查：

```sql
SELECT COUNT(*) FROM study_comment
WHERE post_id = ? AND deleted_at IS NULL;

SELECT comment_count FROM post
WHERE post_id = ?;
```

### 9.5 运营并发审核

先准备一条 `audit_status=0` 的评论，再跑：

```bash
BASE_URL=$BASE_URL \
SCENARIO=audit-approve \
COMMENT_ID=$PENDING_COMMENT_ID \
OPERATOR_ID=$OPERATOR_ID \
VUS=20 \
DURATION=10s \
ALLOW_BUSINESS_ERRORS=true \
MAX_P95_MS=2000 \
MAX_P99_MS=4000 \
REQUEST_ID_PREFIX=audit-approve \
SUMMARY_PATH=reports/k6/audit-approve.json \
k6 run tools/k6/comment-service.js
```

反查：

```sql
SELECT comment_id, audit_status, visible_status, manual_operator_id
FROM study_comment
WHERE comment_id = ?;
```

验收：

- 不允许 5xx。
- 最终只能审核成功一次。
- 其他请求应该是业务冲突，不应该造成重复状态变更。

## 10. 第六阶段：阶梯压测

前面都稳定后，再跑阶梯：

```bash
BASE_URL=$BASE_URL \
SCENARIO=student-post-detail \
POST_IDS="$POST_IDS" \
POST_ID_MODE=hotspot_80_20 \
HOT_TRAFFIC_PERCENT=80 \
HOT_POST_PERCENT=20 \
HOT_POST_PICK=hash \
HOT_POST_SEED=comment-service-8020 \
PAGE_SIZE=20 \
STAGES=30s:10,1m:30,1m:50,30s:0 \
MAX_P95_MS=1500 \
MAX_P99_MS=3000 \
TRACE_SAMPLES_PER_VU=1 \
REQUEST_ID_PREFIX=detail-8020-ramp \
SUMMARY_PATH=reports/k6/detail-8020-ramp.json \
k6 run tools/k6/comment-service.js
```

重点看：

- VU 增加时 p95/p99 是否明显抬升。
- 是否出现 5xx。
- 是否出现 Redis/MySQL 连接池等待。
- Jaeger 慢 span 是否集中在某一类操作。

## 11. 你发结果给我的格式

每跑完一次，把下面内容发给我：

```text
测试名称：
命令：
k6 summary：
trace_id 样例：
Jaeger 最慢 span：
是否有 4xx / 5xx：
SQL 反查结果：
你的观察：
```

至少要包含：

```text
requests
http_failed
checks_rate
latency_avg
latency_p50
latency_p90
latency_p95
latency_p99
trace_id
```

我收到后会追加到 [压测记录](load-test-record.md)，并给出问题判断、原因分析、下一轮建议。
