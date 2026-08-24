# 内部 gRPC mTLS 落地说明

## 目标

内部服务之间不只依赖内网地址或服务发现名称，而是在 gRPC 连接建立阶段完成双向身份认证：

```text
BFF / mall-service / agent-service
 -> 携带自己的 client certificate
 -> 调用 comment-service
 -> comment-service 校验客户端证书由可信 CA 签发，并且服务身份在允许列表中
```

这层解决的是“调用方服务身份是否可信”，用户是否有权限操作资源仍然由业务层校验。

## 本地证书

本地开发使用自签 CA：

```bash
./scripts/gen-mtls-certs.sh
```

证书生成到：

```text
certs/mtls/ca.crt
certs/mtls/comment-service.crt
certs/mtls/comment-service.key
certs/mtls/student-bff.crt
certs/mtls/student-bff.key
certs/mtls/tutor-bff.crt
certs/mtls/tutor-bff.key
certs/mtls/operator-bff.crt
certs/mtls/operator-bff.key
```

默认不会覆盖已有证书。需要重新生成时：

```bash
FORCE=1 ./scripts/gen-mtls-certs.sh
```

生产环境不要使用这套自签脚本，应该接入公司 CA、Kubernetes Secret、cert-manager、Istio、Linkerd 或 SPIFFE/SPIRE。

## comment-service 服务端配置

开启 gRPC mTLS：

```yaml
server:
  grpc:
    addr: 0.0.0.0:9000
    timeout: 3s
    tls:
      enabled: true
      ca_file: ../certs/mtls/ca.crt
      cert_file: ../certs/mtls/comment-service.crt
      key_file: ../certs/mtls/comment-service.key
      allowed_clients:
        - student-bff
        - tutor-bff
        - operator-bff
```

服务端会执行两层校验：

```text
1. 客户端证书必须由 ca.crt 对应 CA 签发；
2. 客户端证书中的 CN / DNS SAN / URI 必须命中 allowed_clients。
```

## BFF 客户端配置

student-bff 调 comment-service 时开启 mTLS：

```yaml
client:
  comment_service:
    service_name: comment-service
    timeout: 2s
    tls:
      enabled: true
      ca_file: ../certs/mtls/ca.crt
      cert_file: ../certs/mtls/student-bff.crt
      key_file: ../certs/mtls/student-bff.key
      server_name: comment-service
```

`server_name` 必须和 `comment-service.crt` 里的 DNS SAN 匹配，否则 TLS 握手会失败。

## 复用到 mall / agent 服务

复用方式很简单：

```text
作为服务端：
  加载 server cert/key
  配置 ClientAuth = RequireAndVerifyClientCert
  配置 ClientCAs
  配置 allowed_clients

作为客户端：
  加载 client cert/key
  配置 RootCAs
  配置 ServerName
  使用 Kratos grpc.Dial + WithTLSConfig
```

面试话术：

> 我把内部 gRPC 调用从明文连接升级成可配置 mTLS。服务端要求调用方必须提供由可信 CA 签发的客户端证书，并校验证书里的服务身份是否在 allowed_clients 中；客户端也会校验服务端证书和 server_name。这样 BFF、mall、agent、comment-service 之间的调用在进入业务 handler 前，就已经完成服务身份认证和传输加密。
