# Service

`internal/service` 是 comment-service 的 RPC/HTTP 入口层，负责 proto DTO 和业务模型之间的转换。

主要职责：

- 实现学生端、助教端、运营端的 Kratos service。
- 将请求参数透传给 biz/usecase。
- 将业务错误映射成明确 HTTP/gRPC 错误，例如重复取消点赞返回 409、点赞短锁忙返回 429。
- 通过 mapper 统一组装对外响应 DTO，避免 data/biz 层感知接口结构。
