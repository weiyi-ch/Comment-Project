# comment-tutor

`comment-tutor` 是助教端 BFF，负责助教端 HTTP 接口、access token 鉴权、发帖、帖子管理、评论管理、回复和搜索入口，并通过 gRPC 调用 `comment-service`。

## 运行链路

```text
Tutor HTTP
 -> comment-tutor
 -> Consul discovery
 -> comment-service gRPC
```

`comment-tutor` 启动后会注册到 Consul；调用核心服务时，如果没有配置直连 `endpoint`，就使用 `discovery:///comment-service` 从 Consul 发现实例。

## 本地启动

```bash
docker compose up -d consul mysql redis elasticsearch kafka kafka-ui canal

cd comment-service
go run ./cmd/comment-service -conf ./configs

cd ../comment-tutor
go run ./cmd/comment-tutor -conf ./configs/config.yaml
```

默认地址：

```text
http://127.0.0.1:8082
```

健康检查：

```bash
curl http://127.0.0.1:8082/health
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
POST   /api/tutor/auth/register
POST   /api/tutor/auth/login
POST   /api/tutor/auth/refresh
POST   /api/tutor/auth/logout
GET    /api/tutor/posts
POST   /api/tutor/posts
GET    /api/tutor/posts/{post_id}
PUT    /api/tutor/posts/{post_id}
DELETE /api/tutor/posts/{post_id}
GET    /api/tutor/posts/{post_id}/comments
GET    /api/tutor/comments/{comment_id}
DELETE /api/tutor/comments/{comment_id}
POST   /api/tutor/comments/{comment_id}/reply
DELETE /api/tutor/replies/{reply_id}
GET    /api/tutor/search/posts
GET    /api/tutor/posts/{post_id}/comments/search
```

## English Notes

`comment-tutor` is the tutor-facing BFF. It exposes tutor HTTP APIs for post management, comment management, replies, and search, then calls `comment-service` through gRPC.

Runtime lookup uses Consul:

```text
comment-tutor -> Consul discovery -> comment-service -> gRPC
```

Default address: `http://127.0.0.1:8082`.
