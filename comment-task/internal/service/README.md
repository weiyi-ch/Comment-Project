# Service

`internal/service` 目前保留 Kratos 模板中的基础 service。`comment-task` 的主要入口是 Kratos Server 生命周期中的 `JobWorker`。

如果后续需要为任务服务暴露健康检查、手动补偿、计数校准等管理接口，可以在这里扩展。
