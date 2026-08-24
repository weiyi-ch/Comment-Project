# Biz

`internal/biz` 是 comment-service 的业务编排层，定义 usecase、repo 接口和业务错误。

主要职责：

- 承接 service 层请求，调用 repo 完成核心业务。
- 定义跨层可识别的业务错误，例如 `ErrPostLikeBusy`、`ErrPostNotLiked`。
- 不直接操作 MySQL、Redis、Elasticsearch，也不拼接对外 DTO。
