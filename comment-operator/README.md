# comment-operator

`comment-operator` 是运营端 BFF，负责运营端 HTTP 接口、access token 鉴权、评论审核、审核列表和运营搜索入口，并通过 gRPC 调用 `comment-service`。

## 运行链路

```text
Operator HTTP
 -> comment-operator
 -> Consul discovery
 -> comment-service gRPC
```

`comment-operator` 启动后会注册到 Consul；调用核心服务时，如果没有配置直连 `endpoint`，就使用 `discovery:///comment-service` 从 Consul 发现实例。

## 本地启动

```bash
docker compose up -d consul mysql redis elasticsearch kafka kafka-ui canal

cd comment-service
go run ./cmd/comment-service -conf ./configs

cd ../comment-operator
go run ./cmd/comment-operator -conf ./configs/config.yaml
```

默认地址：

```text
http://127.0.0.1:8083
```

健康检查：

```bash
curl http://127.0.0.1:8083/health
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
POST   /api/operator/auth/register
POST   /api/operator/auth/login
POST   /api/operator/auth/refresh
POST   /api/operator/auth/logout
GET    /api/operator/posts/{post_id}
GET    /api/operator/comments
GET    /api/operator/comments/{comment_id}
POST   /api/operator/comments/{comment_id}/audit
GET    /api/operator/search/comments
```

## English Notes

`comment-operator` is the operator-facing BFF. It exposes operator HTTP APIs for comment moderation, moderation lists, and operator search, then calls `comment-service` through gRPC.

Runtime lookup uses Consul:

```text
comment-operator -> Consul discovery -> comment-service -> gRPC
```

Default address: `http://127.0.0.1:8083`.
