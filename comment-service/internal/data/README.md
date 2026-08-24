# Data

`internal/data` 是 comment-service 的数据访问层，负责 MySQL、Redis、Elasticsearch、Kafka producer 以及缓存一致性相关逻辑。

主要职责：

- 通过 gorm/gen query 访问 MySQL。
- 读路径使用 Redis cache-aside、Bloom Filter、singleflight 降低回源压力。
- 写路径在状态变更后删除相关缓存。
- 点赞/取消点赞同步写 `post_like` 事实关系，`post.like_count` 只写 Redis delta，由 `comment-task` 异步落库。
- 搜索接口读 Elasticsearch，详情和权限判断仍回到 MySQL/Redis 主链路。
