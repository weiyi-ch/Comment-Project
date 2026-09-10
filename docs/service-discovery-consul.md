# 服务注册与发现 / Service Registration And Discovery

## 中文说明

本项目使用 Kratos + Consul 做服务注册与发现。`comment-service` 是领域核心服务，启动后把自己的 HTTP/gRPC endpoint 注册到 Consul；`comment-student`、`comment-tutor`、`comment-operator` 三端 BFF 通过 Consul 发现 `comment-service`，再用 gRPC 调用核心服务。

### 解决的问题

如果 BFF 直接写死 `127.0.0.1:9000`，本地单机可以跑，但一旦服务迁移端口、扩容多个实例、或者容器内部地址和宿主机地址不同，调用方都要改配置。注册中心的作用是把“服务名”和“实例地址”解耦：

```text
BFF 只知道 comment-service
Consul 返回可用实例 endpoint
Kratos gRPC client 连接具体实例
```

### 本地依赖

`docker-compose.yml` 已启动 Consul：

```yaml
consul:
  image: swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/hashicorp/consul:1.20.1
  command: ["agent", "-dev", "-client=0.0.0.0", "-ui"]
  ports:
    - "8500:8500"
```

本地访问地址：

```text
Consul UI: http://127.0.0.1:8500
Consul API: http://127.0.0.1:8500
```

### 注册链路

`comment-service` 的配置：

```yaml
registry:
  consul:
    address: 127.0.0.1:8500
    scheme: http
```

核心代码在 [comment-service/internal/server/server.go](/Users/yaowy/GolandProjects/comment/comment-service/internal/server/server.go:16)：

```go
func NewRegistrar(conf *conf.Registry) registry.Registrar {
    c := api.DefaultConfig()
    c.Address = conf.Consul.Address
    c.Scheme = conf.Consul.Scheme
    client, err := api.NewClient(c)
    if err != nil {
        panic(err)
    }
    return consul.New(client)
}
```

`newApp` 把 registrar 注入 Kratos App，位置在 [comment-service/cmd/comment-service/main.go](/Users/yaowy/GolandProjects/comment/comment-service/cmd/comment-service/main.go:48)：

```go
return kratos.New(
    kratos.ID(id),
    kratos.Name(Name),
    kratos.Version(Version),
    kratos.Server(gs, hs),
    kratos.Registrar(r),
)
```

服务启动时，Kratos 会把 `comment-service`、实例 ID、版本、endpoint 注册到 Consul。Kratos Consul registry 默认开启 TCP health check 和 TTL heartbeat：TCP check 用 endpoint 判断端口是否可连，TTL heartbeat 用注册器定期续约；进程停止或检查失败后，Consul 会把实例标记为不健康，后续发现默认只返回 passing 实例。

### 发现链路

三端 BFF 的配置：

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

BFF 构造 Consul registry，位置示例：[comment-student/internal/server/server.go](/Users/yaowy/GolandProjects/comment/comment-student/internal/server/server.go:25)：

```go
func NewConsulRegistry(cfg conf.Registry) *consul.Registry {
    c := api.DefaultConfig()
    c.Address = cfg.Consul.Address
    c.Scheme = cfg.Consul.Scheme
    client, err := api.NewClient(c)
    if err != nil {
        panic(err)
    }
    return consul.New(client)
}

func NewDiscovery(reg *consul.Registry) registry.Discovery {
    return reg
}
```

gRPC client 发现 `comment-service`，位置示例：[comment-student/internal/client/comment.go](/Users/yaowy/GolandProjects/comment/comment-student/internal/client/comment.go:31)：

```go
if endpoint == "" {
    endpoint = fmt.Sprintf("discovery:///%s", cfg.ServiceName)
    opts = append(opts, kgrpc.WithDiscovery(discovery))
}
opts = append([]kgrpc.ClientOption{kgrpc.WithEndpoint(endpoint)}, opts...)
```

这里的 `discovery:///comment-service` 是 Kratos 约定的服务发现 endpoint。BFF 不需要知道 `comment-service` 当前 IP 和端口，只要 Consul 里有 passing 实例即可。

### 调用链路

```text
HTTP request
 -> comment-student/comment-tutor/comment-operator
 -> access token 校验并生成 Principal
 -> identityForwarder 写入 gRPC metadata
 -> Consul discovery 解析 discovery:///comment-service
 -> gRPC 调用 comment-service
 -> comment-service authctx middleware 读取可信 metadata
 -> 进入领域 service/usecase/repo
```

身份透传核心代码在三端 BFF 的 `internal/client/comment.go`：

```go
ctx = metadata.AppendToOutgoingContext(
    ctx,
    "x-user-id", strconv.FormatInt(p.UserID, 10),
    "x-role", p.Role,
    "x-token-id", p.TokenID,
)
```

注意：`comment-service` 不信任外部 HTTP 请求里的 `x-user-id`，它信任的是三端 BFF 鉴权后通过内部 gRPC metadata 传入的身份。

### 本地启动顺序

```bash
docker compose up -d consul mysql redis elasticsearch kafka kafka-ui canal

cd comment-service
go run ./cmd/comment-service -conf ./configs

cd ../comment-student
go run ./cmd/comment-student -conf ./configs/config.yaml

cd ../comment-tutor
go run ./cmd/comment-tutor -conf ./configs/config.yaml

cd ../comment-operator
go run ./cmd/comment-operator -conf ./configs/config.yaml
```

### 排查命令

```bash
curl http://127.0.0.1:8500/v1/catalog/services
curl 'http://127.0.0.1:8500/v1/health/service/comment-service?passing=true'
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:8082/health
curl http://127.0.0.1:8083/health
```

如果 BFF 启动时报 `service comment-service not found in registry`，通常是 `comment-service` 还没启动、没有注册成功、Consul 地址不一致，或者 Consul 里实例健康检查没有 passing。

## English Notes

This project uses Kratos + Consul for service registration and discovery. `comment-service` registers its HTTP/gRPC endpoints in Consul. The three BFF services discover `comment-service` by name and call it through generated gRPC clients.

The key design goal is to decouple callers from concrete instance addresses. BFF services use `discovery:///comment-service`; Kratos asks Consul for healthy instances and then dials the selected endpoint.

Startup order:

```bash
docker compose up -d consul mysql redis elasticsearch kafka kafka-ui canal
cd comment-service && go run ./cmd/comment-service -conf ./configs
cd ../comment-student && go run ./cmd/comment-student -conf ./configs/config.yaml
cd ../comment-tutor && go run ./cmd/comment-tutor -conf ./configs/config.yaml
cd ../comment-operator && go run ./cmd/comment-operator -conf ./configs/config.yaml
```

Useful checks:

```bash
curl http://127.0.0.1:8500/v1/catalog/services
curl 'http://127.0.0.1:8500/v1/health/service/comment-service?passing=true'
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:8082/health
curl http://127.0.0.1:8083/health
```
