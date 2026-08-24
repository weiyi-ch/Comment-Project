# comment-task

`comment-task` 是评论系统的后台任务服务，和 `comment-service` 共用 MySQL、Redis、Kafka、Elasticsearch 环境。

## 当前职责

- 消费 Canal 写入的 Kafka topic `comment-service`，先按 ES 中的 `sync_binlog_file/sync_binlog_pos` 判断消息是否过期，再回查 MySQL 最新数据同步到 Elasticsearch。
- 消费计数 dirty topic `postlike`，为点赞数和评论数计算下一次刷库时间，并写入 Redis 调度 zset。
- 后台扫描 Redis 调度 zset，到期后再次校验 `dirty_at/dirty_since`，再领取 delta 异步更新 `post.like_count/comment_count`。
- 低频扫描 Redis dirty 队列，补偿 Kafka dirty 通知发送失败或消费失败。

## 关键 Redis Key

| key | 类型 | 作用 |
| --- | --- | --- |
| `counter:post_like:delta` | Hash | field 是 `post_id`，value 是待落库的 `+1/-1` 聚合 delta |
| `queue:post_like:dirty` | Set | 待处理帖子 ID 去重集合 |
| `queue:post_like:dirty_at` | ZSet | 记录每个帖子最后一次点赞/取消点赞时间 |
| `queue:post_like:dirty_since` | ZSet | 记录本轮 dirty 周期第一次变化时间，避免热点帖子无限延迟 |
| `queue:post_like:flush_schedule` | ZSet | task 计算出的刷库调度队列，member 是 `post_id`，score 是 `nextFlushTime` |
| `counter:post_like:processing` | Hash | 已领取但未确认落库的 delta，读详情时需要一起叠加 |

评论数使用同构 key，把 `post_like` 替换为 `post_comment`。

## Kafka Topic

| topic | 来源 | 用途 |
| --- | --- | --- |
| `comment-service` | Canal | MySQL row change -> Elasticsearch |
| `postlike` | comment-service | 点赞/取消点赞/评论数计数 dirty 通知 |

`postlike` 只承载 `post_id` dirty 通知，真实计数变化以 Redis hash 中的 delta 为准。Kafka 消息只负责唤醒 task 计算 zset 调度时间，不直接触发刷库。

## ES 同步可靠性

- Canal 消息只作为变更通知，ES 文档内容以 MySQL 当前最新数据为准。
- ES 文档保存 `sync_binlog_file` 和 `sync_binlog_pos`，旧消息或重复消息会被跳过。
- Kafka offset 使用手动提交：ES 写成功，或者失败事件可靠写入 `es_sync_retry` 后才提交。
- `es_sync_retry` 后台重试 3 次，仍失败则进入 `es_sync_dlq`。

## 本地常用命令

```bash
# 生成 proto / wire / 依赖整理
make all

# 编译和测试
go test ./...

# 启动后台任务
kratos run
```

## 配置说明

- `configs/config.yaml` 中的 MySQL、Redis、Kafka 必须和 `comment-service` 使用同一套环境。
- Redis 如果设置了密码，需要配置 `data.redis.pass`。
- `POST_LIKE_COUNT_KAFKA_TOPIC` 可以覆盖点赞 dirty topic，默认是 `postlike`。
- `POST_LIKE_COUNT_KAFKA_GROUP_ID` 可以覆盖点赞 dirty 消费组，默认是 `{kafka.group_id}-postlike`。

## 设计文档

点赞计数异步落库的完整设计见 [comment-service/docs/post-like-async-counter-design.md](../comment-service/docs/post-like-async-counter-design.md)。
