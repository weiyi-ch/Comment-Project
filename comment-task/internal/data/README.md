# Data

`internal/data` 是 comment-task 的资源层，负责初始化后台任务需要的 MySQL 和 Redis 连接。

主要职责：

- 连接 MySQL，用于异步更新 `post.like_count`。
- 连接 Redis，用于读取 `counter:post_like:delta`、`queue:post_like:dirty`、`queue:post_like:dirty_at`。
- 在进程退出时关闭 Redis 和 MySQL 连接池。
