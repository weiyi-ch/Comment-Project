# comment-student

`comment-student` 是学生端 BFF，负责学生端 HTTP 接口、access token 鉴权、多维令牌桶限流，以及通过 gRPC 调用 `comment-service`。

## 运行链路

```text
Student HTTP
 -> comment-student
 -> Consul discovery
 -> comment-service gRPC
```

`comment-student` 启动后也会注册到 Consul；调用核心服务时，如果没有配置直连 `endpoint`，就使用 `discovery:///comment-service` 从 Consul 发现实例。

## 本地启动

先启动基础组件和 `comment-service`：

```bash
docker compose up -d consul mysql redis elasticsearch kafka kafka-ui canal

cd comment-service
go run ./cmd/comment-service -conf ./configs
```

再启动学生端：

```bash
cd ../comment-student
go run ./cmd/comment-student -conf ./configs/config.yaml
```

默认地址：

```text
http://127.0.0.1:8081
```

健康检查：

```bash
curl http://127.0.0.1:8081/health
```

## 配置

```yaml
registry:
  consul:
    address: 127.0.0.1:8500
    scheme: http
client:
  comment_service:
    service_name: comment-service
    timeout: 2s
```

## 主要接口

```text
POST   /api/student/auth/register
POST   /api/student/auth/login
POST   /api/student/auth/refresh
POST   /api/student/auth/logout
GET    /api/student/posts/{post_id}
GET    /api/student/posts/{post_id}/comments
POST   /api/student/posts/{post_id}/comments
GET    /api/student/comments
GET    /api/student/comments/{comment_id}
DELETE /api/student/comments/{comment_id}
POST   /api/student/posts/{post_id}/like
DELETE /api/student/posts/{post_id}/like
GET    /api/student/search/posts
GET    /api/student/search/comments
```

## English Notes

`comment-student` is the student-facing BFF. It exposes student HTTP APIs, validates access tokens, applies multi-dimensional rate limiting, and calls `comment-service` through gRPC.

Runtime lookup uses Consul:

```text
comment-student -> Consul discovery -> comment-service -> gRPC
```

Default address: `http://127.0.0.1:8081`.
