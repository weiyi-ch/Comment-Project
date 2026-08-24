# Biz

`internal/biz` 目前保留 Kratos 模板中的基础 usecase。后台任务的核心逻辑主要在 `internal/task`。

后续如果增加更多离线任务，可以把跨任务的业务抽象放到这里，保持 `task` 层只做调度和消费循环。
