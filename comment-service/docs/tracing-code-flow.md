# comment-service 埋点代码流程说明

这份文档专门解释当前链路追踪代码怎么跑起来，以及 `StudentService.GetPostDetailStudent` 这条核心链路为什么能在 Jaeger 里展开到 Service、Usecase、Repo、Redis、MySQL。

## 1. 埋点目标

普通日志只能告诉我们“这个接口慢了”，链路追踪要回答更细的问题：

- 请求总耗时是多少。
- 慢在 service、biz、data 哪一层。
- Redis 是否命中。
- 缓存未命中后是否回源 MySQL。
- MySQL 查询耗时和错误是否出现在同一个 trace 里。

当前项目接入了两类能力：

| 能力 | 代码位置 | 作用 |
| --- | --- | --- |
| 入口 trace | `internal/server/http.go` / `internal/server/grpc.go` | 请求进来时生成或继承 trace |
| trace exporter | `internal/observability/tracing.go` | 把 span 通过 OTLP 发到 Jaeger/Collector |
| access log | `internal/middleware/accesslog/accesslog.go` | 打印并回写 `x-trace-id`、`x-request-id` |
| 手动子 span | `internal/observability/span.go` | 让内部方法出现在 Jaeger 瀑布图里 |
| 学生详情链路埋点 | `internal/service/student.go`、`internal/biz/student.go`、`internal/data/student.go`、`internal/data/reply_batch.go` | 展开帖子详情核心链路 |

## 2. 启动时发生了什么

入口在 `cmd/comment-service/main.go`。

启动流程：

```text
main
  -> 初始化 logger
       -> 日志字段挂载 tracing.TraceID()
       -> 日志字段挂载 tracing.SpanID()
  -> observability.InitTracer(...)
       -> 读取环境变量
       -> 创建 OTLP exporter
       -> 创建 OpenTelemetry TracerProvider
       -> 设置全局 propagator
  -> 加载配置
  -> wireApp 组装 server/data/biz/service
  -> app.Run()
```

关键环境变量：

| 变量 | 示例 | 说明 |
| --- | --- | --- |
| `TRACE_ENABLED` | `true` | 显式启用 trace exporter |
| `OTEL_SERVICE_NAME` | `comment-service` | Jaeger UI 中展示的服务名 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `127.0.0.1:4317` | OTLP gRPC 地址 |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `127.0.0.1:4317` | traces 专用地址，优先级更高 |
| `TRACE_SAMPLE_RATIO` | `1` | 采样率，本地演示用 1 |

如果不设置 exporter 环境变量，服务仍然能正常启动，只是 Jaeger 收不到 span。日志里仍会有入口 trace 字段。

## 3. 请求入口怎么生成 trace

HTTP 和 gRPC 都配置了同样的中间件顺序：

```text
recovery.Recovery()
tracing.Server()
accesslog.Server(logger)
validate.Validator()
```

每个中间件职责：

| 中间件 | 作用 |
| --- | --- |
| `recovery` | panic 兜底，避免进程崩溃 |
| `tracing.Server()` | 从 `traceparent` 继承 trace，或新建 server span |
| `accesslog.Server()` | 请求结束后记录 trace、状态码、耗时，并写回响应头 |
| `validate.Validator()` | 按 proto validate 规则校验参数 |

请求进来后，Kratos `tracing.Server()` 会把当前 span 放进 `context.Context`。后续所有方法只要继续传递这个 `ctx`，手动创建的 span 就会成为入口 span 的子 span。

## 4. accesslog 做了什么

`internal/middleware/accesslog/accesslog.go` 在业务 handler 执行后读取当前 span：

```text
trace.SpanContextFromContext(ctx)
```

然后做三件事：

1. 拿到 `trace_id` 和 `span_id`。
2. 写回响应头：

   ```text
   x-trace-id: <trace id>
   x-request-id: <请求传入的 request id，没传时用 trace id>
   ```

3. 打印结构化访问日志：

   ```text
   event=access
   operation=/api.comment.v1.StudentService/GetPostDetailStudent
   request_id=...
   trace_id=...
   span_id=...
   code=...
   latency_ms=...
   ```

Postman 里推荐只传：

```http
x-request-id: postman-detail-001
```

`x-trace-id` 不需要自己填，服务端会自动生成并返回。

## 5. 手动 span 封装

手动埋点统一封装在 `internal/observability/span.go`：

```go
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)
func EndSpan(span trace.Span, err error)
```

`StartSpan` 做两件事：

- 使用全局 tracer `comment-service/manual` 创建子 span。
- 把传入的 attributes 写到 span 上。

`EndSpan` 做两件事：

- 如果 `err != nil`，调用 `span.RecordError(err)`。
- 设置 error status 后结束 span。

为什么很多方法改成命名返回：

```go
func (...) (reply *pb.GetPostDetailReply, err error) {
    ctx, span := observability.StartSpan(ctx, "StudentService.GetPostDetailStudent")
    defer func() { observability.EndSpan(span, err) }()
    ...
}
```

因为 `defer` 在函数退出时才能看到最终的 `err`。这样不管中间哪一步返回错误，都能把错误记录到对应 span。

## 6. 学生查看帖子详情的完整 span 树

当前 `GetPostDetailStudent` 的 span 树：

```text
HTTP server span
operation=/api.comment.v1.StudentService/GetPostDetailStudent
  -> StudentService.GetPostDetailStudent
     -> StudentUsecase.GetPostDetailStudent
        -> studentRepo.GetPostDetailWithComments
           -> studentRepo.GetPostByID
              -> redis.GET mysql:post
              -> redis.GET mysql:post.double_check
              -> mysql.SELECT post
           -> listPostCommentsStudentForDetail
              -> post.comment_count == 0 时跳过评论表
              -> redis.GET student:post_comments.detail
              -> redis.GET student:post_comments.detail.double_check
              -> mysql.SELECT study_comment.items
     -> StudentUsecase.GetRepliesByCommentIDs
        -> studentRepo.GetRepliesByCommentIDs
           -> mysql.SELECT study_comment_reply.batch
```

如果 Redis 命中，对应 MySQL span 不会出现，这是正常现象。链路追踪看到的是“真实执行过的步骤”，缓存命中意味着没有回源 MySQL。

## 7. 每一层具体埋了什么

### 7.1 Service 层

方法：

```text
StudentService.GetPostDetailStudent
```

代码位置：

```text
internal/service/student.go
```

span attributes：

| 字段 | 含义 |
| --- | --- |
| `post_id` | 请求的帖子 ID |
| `user_id` | 当前学生/用户 ID |
| `comment_page_num` | 评论页码 |
| `comment_page_size` | 评论每页数量 |
| `comments.count` | 本次返回评论数量 |
| `reply_groups.count` | 本次查到回复分组数量 |
| `total_comments` | 当前帖子评论总数 |

返回结构：

```text
GetPostDetailReply
  post: PostDTO
  comments: []CommentDTO
  total_comments: int64
```

`PostDTO` 关键字段：

```text
post_id, author_id, title, content, status, like_count, comment_count, created_at
```

`CommentDTO` 关键字段：

```text
comment_id, post_id, student_id, content, visible_status, audit_status, created_at, replies
```

`ReplyDTO` 关键字段：

```text
comment_reply_id, comment_id, tutor_id, content, created_at
```

### 7.2 Usecase 层

方法：

```text
StudentUsecase.GetPostDetailStudent
StudentUsecase.GetRepliesByCommentIDs
```

代码位置：

```text
internal/biz/student.go
```

`GetPostDetailStudent` 负责：

- 修正分页参数。
- 调用 `repo.GetPostDetailWithComments`。
- 返回 `post, comments, total` 给 service。

span attributes：

| 字段 | 含义 |
| --- | --- |
| `post_id` | 帖子 ID |
| `page_num` | 页码 |
| `page_size` | 每页数量 |
| `comments.count` | 当前页评论数 |
| `total_comments` | 总评论数 |

`GetRepliesByCommentIDs` 负责：

- 接收当前页评论 ID。
- 一次性批量加载回复，避免 N+1。

span attributes：

| 字段 | 含义 |
| --- | --- |
| `comment_ids.count` | 当前页评论 ID 数量 |
| `reply_groups.count` | 返回的回复分组数量 |

### 7.3 Repo 聚合层

方法：

```text
studentRepo.GetPostDetailWithComments
```

代码位置：

```text
internal/data/student.go
```

它本身不直接查 MySQL，而是组合两个已有查询：

```text
GetPostByID
ListPostCommentsStudent
```

span attributes：

| 字段 | 含义 |
| --- | --- |
| `post_id` | 帖子 ID |
| `page_num` | 页码 |
| `page_size` | 每页数量 |
| `comments.count` | 当前页评论数 |
| `total_comments` | 总评论数 |

### 7.4 帖子对象查询

方法：

```text
studentRepo.GetPostByID
```

真实数据流：

```text
Bloom bf:post
  -> redis.GET mysql:post:{post_id}
  -> singleflight
       -> redis.GET mysql:post:{post_id} double check
       -> mysql.SELECT post
       -> Redis SET mysql:post:{post_id}
```

span：

| span | 什么时候出现 | 关键 attributes |
| --- | --- | --- |
| `studentRepo.GetPostByID` | 每次调用都会出现 | `post_id`, `cache.hit` |
| `redis.GET mysql:post` | 第一次读对象缓存 | `db.system=redis`, `db.operation=GET`, `cache.key`, `cache.hit` |
| `redis.GET mysql:post.double_check` | singleflight 内二次检查缓存 | `cache.key`, `cache.hit` |
| `mysql.SELECT post` | 缓存未命中后回源 | `db.system=mysql`, `db.operation=SELECT`, `db.table=post`, `post_id` |

MySQL 查询条件：

```text
post_id = ?
status = 1
deleted_at IS NULL
```

返回结构：

```text
*model.Post
```

### 7.5 详情页评论列表查询

方法：

```text
studentRepo.listPostCommentsStudentForDetail
```

真实数据流：

```text
post.comment_count == 0
  -> 直接返回空评论列表，不查 study_comment

post.comment_count > 0
  -> redis.GET mysql:student:post_comments:{post_id}:{hash}
       -> hit: 得到 comment_ids 和 total
       -> redis.MGET mysql:comment:{comment_id}
       -> mysql.SELECT study_comment.batch_by_ids, 只补对象缓存 miss 的评论
  -> singleflight
       -> redis.GET mysql:student:post_comments:{post_id}:{hash} double check
       -> mysql.SELECT study_comment.items
       -> Redis SET mysql:comment:{comment_id}
       -> Redis SET list id cache
```

span：

| span | 什么时候出现 | 关键 attributes |
| --- | --- | --- |
| `redis.GET student:post_comments.detail` | 第一次读详情页评论列表缓存 | `db.system=redis`, `db.operation=GET`, `cache.key`, `cache.hit` |
| `redis.GET student:post_comments.detail.double_check` | singleflight 内二次检查缓存 | `cache.key`, `cache.hit` |
| `redis.MGET mysql:comment` | 列表缓存命中后批量读取评论对象 | `app.role`, `cache.key.prefix`, `cache.keys.count`, `cache.hit.count`, `cache.miss.count` |
| `mysql.SELECT study_comment.items` | 缓存未命中且 `post.comment_count > 0` 时回源 | `db.system=mysql`, `db.operation=SELECT`, `db.table=study_comment`, `total.source=post.comment_count`, `comments.count` |
| `mysql.SELECT study_comment.batch_by_ids` | 列表 ID 命中但评论对象部分 miss 时补查 | `db.table=study_comment`, `comment_ids.count`, `comments.count` |

单独的学生评论列表接口仍使用 `studentRepo.ListPostCommentsStudent`，它需要返回精确 total，所以缓存未命中时仍会使用 `mysql.SELECT study_comment.list` 做分页和 total 查询。详情页已经有 `post.comment_count`，因此不再额外 `COUNT(*)`。

MySQL 查询条件：

```text
post_id = ?
visible_status = 1
deleted_at IS NULL
ORDER BY created_at DESC
LIMIT offset, page_size
```

返回结构：

```text
[]*model.StudyComment
total int64
```

### 7.6 批量回复查询

方法：

```text
StudentUsecase.GetRepliesByCommentIDs
  -> studentRepo.GetRepliesByCommentIDs
  -> mysql.SELECT study_comment_reply.batch
```

代码位置：

```text
internal/data/reply_batch.go
```

真实数据流：

```text
normalize comment_ids
  -> 逐个读 mysql:comment_replies:{comment_id}
  -> missed comment_ids 进入 singleflight
  -> MySQL WHERE comment_id IN (...)
  -> 按 comment_id 拆分写回每个回复列表缓存
  -> 返回 map[comment_id][]reply
```

span：

| span | 关键 attributes |
| --- | --- |
| `studentRepo.GetRepliesByCommentIDs` | `comment_ids.count`, `reply_groups.count` |
| `mysql.SELECT study_comment_reply.batch` | `db.system=mysql`, `db.operation=SELECT`, `db.table=study_comment_reply`, `comment_ids.count`, `replies.count` |

返回结构：

```text
map[int64][]*model.StudyCommentReply
```

## 8. 为什么要传递 ctx

每个方法创建 span 后都使用新的 `ctx` 继续调用下游：

```go
ctx, span := observability.StartSpan(ctx, "StudentUsecase.GetPostDetailStudent")
...
post, comments, total, err = uc.repo.GetPostDetailWithComments(ctx, postID, pageNum, pageSize)
```

这个 `ctx` 里包含当前 span。如果不把新 `ctx` 传下去，下游 span 就可能挂不到正确父节点下，Jaeger 里会变成断开的链路。

面试回答：

> 链路追踪本质是通过 context 传播 trace/span 上下文。入口 middleware 创建 server span 后，service、biz、data 每层都用传入 ctx 创建子 span，并把新 ctx 继续传给下游，这样 Jaeger 里才能形成一棵完整调用树。

## 9. 错误如何体现在 Jaeger

所有手动 span 结束时调用：

```go
observability.EndSpan(span, err)
```

如果 `err != nil`：

- `span.RecordError(err)` 记录错误事件。
- `span.SetStatus(codes.Error, err.Error())` 标记错误状态。

因此当 MySQL 查询失败、缓存操作失败且返回错误、业务链路返回错误时，Jaeger 里对应 span 会显示 error。

注意：当前有些 Redis 读取失败会被业务降级处理，只写 warn 日志，不一定向上返回 error。这类场景 span 不一定是 error，但可以通过 access log 和业务日志定位。

## 10. 如何继续给其他接口加 span

建议命名规范：

| 层级 | 命名示例 |
| --- | --- |
| service | `TutorService.CreatePost` |
| usecase | `TutorUsecase.CreatePost` |
| repo | `tutorRepo.CreatePost` |
| Redis | `redis.GET mysql:post`、`redis.SET mysql:post` |
| MySQL | `mysql.INSERT post`、`mysql.UPDATE study_comment` |
| ES | `es.SEARCH post` |

推荐 attributes：

```text
db.system=mysql/redis/elasticsearch
db.operation=SELECT/INSERT/UPDATE/GET/SET/SEARCH
db.table=post/study_comment/study_comment_reply
cache.key=...
cache.hit=true/false
post_id/comment_id/user_id/page_num/page_size
```

不要放进 span 的内容：

- 完整评论内容。
- token、密码、手机号等敏感信息。
- 完整 SQL，尤其是带用户输入的 SQL。
- 太大的响应体。

## 11. 本地验证步骤

1. 启动 Jaeger。

   ```bash
   docker run --rm --name jaeger \
     -e COLLECTOR_OTLP_ENABLED=true \
     -p 16686:16686 \
     -p 4317:4317 \
     jaegertracing/all-in-one:latest
   ```

2. 启动服务。

   ```bash
   TRACE_ENABLED=true \
   OTEL_SERVICE_NAME=comment-service \
   OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4317 \
   TRACE_SAMPLE_RATIO=1 \
   go run ./cmd/comment-service -conf ./configs
   ```

3. 用 Postman 或 curl 请求帖子详情。

   ```bash
   curl -i -H 'x-request-id: postman-detail-001' \
     'http://127.0.0.1:8888/v1/student/posts/10001?comment_page_num=1&comment_page_size=20'
   ```

4. 从响应头复制 `x-trace-id`。

5. 打开 Jaeger UI，搜索 `comment-service` 或直接按 trace ID 查询。

## 12. 面试回答

可以这样说：

> 项目里我用 Kratos `tracing.Server()` 负责入口 span，用 OpenTelemetry OTLP exporter 把 span 发到 Jaeger。accesslog 会从 context 里取 trace_id/span_id，打印访问日志并把 `x-trace-id` 写回响应头。为了让 Jaeger 不只看到入口，我又封装了 `observability.StartSpan/EndSpan`，在 `GetPostDetailStudent` 的 service、usecase、repo、Redis GET、MySQL SELECT 都加了手动子 span。这样压测时拿到慢请求的 trace_id，就能看出慢在缓存未命中、MySQL 查询、评论列表还是批量回复查询。
