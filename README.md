# Comment-Project

一个基于 Go Kratos 的学习社区评论系统复盘项目，用于沉淀评论、点赞、审核、搜索、缓存、异步计数、BFF 鉴权和微服务调用链路等后端设计能力。

项目背景来自某在线教育/学习社区业务场景，具体公司与业务名称已做泛化处理。系统面向学生、助教、运营三类角色：学生浏览知识帖、发表评论、点赞和搜索；助教发布帖子、回复评论并管理自己帖子下的互动；运营审核评论、处理违规内容并查询审核列表。

## 中文说明

### 项目目标

这个项目不是只做 CRUD，而是围绕评论系统中面试最容易被追问的工程问题做实现：

- 高并发读：帖子详情、评论列表、搜索结果如何减少 MySQL 回源。
- 热点写：点赞数、评论数如何避免频繁更新同一行造成锁竞争。
- 数据一致性：评论审核、删除、点赞关系、异步计数和 ES 同步如何保证最终正确。
- 权限边界：BFF 做登录态校验，核心服务做资源级权限校验。
- 可恢复性：Kafka、Redis、ES、MySQL 或后台任务异常时如何重试、补偿和对账。

### 系统模块

| 模块 | 说明 |
| --- | --- |
| `comment-service` | 评论领域核心服务，负责帖子、评论、回复、点赞、审核、搜索等主要业务逻辑 |
| `comment-task` | 异步任务服务，负责消费 Kafka、同步 ES、异步刷点赞/评论计数、处理 retry/DLQ |
| `comment-student` | 学生端 HTTP BFF，负责学生端注册登录、鉴权、参数组装，并通过 gRPC 调用 `comment-service` |
| `comment-tutor` | 助教端 HTTP BFF，负责助教端注册登录、鉴权、帖子、评论、回复和搜索入口 |
| `comment-operator` | 运营端 HTTP BFF，负责运营端注册登录、鉴权、审核列表、审核动作和运营检索入口 |
| `docs` | 项目设计、链路分析、压测记录、后续实现清单 |
| `scripts` | 辅助脚本，例如 mTLS 证书生成 |

### 技术栈

- Go + Kratos
- gRPC / HTTP
- Wire 依赖注入
- MySQL / GORM / Gen
- Redis / RedisBloom / Lua
- Kafka
- Elasticsearch
- Canal binlog 同步
- OpenTelemetry / tracing
- k6 压测脚本

### 本地依赖一键启动

项目根目录提供了 `docker-compose.yml`，用于启动本地中间件依赖，不再依赖远程 MySQL/Redis。所有 Docker 镜像优先使用 `docker.aityp.com` 搜索到的镜像站地址，避免直接拉取海外镜像失败。

前置条件：本机已安装 Docker Desktop 或 Docker Engine，并支持 `docker compose` 命令。

一键启动：

```bash
docker compose up -d
```

查看启动状态：

```bash
docker compose ps
```

停止本地依赖：

```bash
docker compose down
```

如果需要清空本地数据并重新初始化 MySQL 表结构：

```bash
docker compose down -v
docker compose up -d
```

本地组件端口：

| 组件 | 地址 |
| --- | --- |
| MySQL | `127.0.0.1:3306`，账号 `root`，密码 `root1234`，库名 `comment` |
| Redis Stack / RedisBloom | `127.0.0.1:6379` |
| Consul | `127.0.0.1:8500` |
| Kafka | `127.0.0.1:9092` |
| Kafka UI | `http://127.0.0.1:8090` |
| Elasticsearch | `http://127.0.0.1:9200` |
| Kibana | `http://127.0.0.1:5601` |

MySQL 首次启动会执行 `comment-service/sql/comment.sql` 初始化表结构。如果已经存在旧 volume，需要先确认是否要保留数据。

当前 `docker-compose.yml` 使用的镜像如下：

| 组件 | 原镜像 | docker.aityp 镜像站地址 |
| --- | --- | --- |
| MySQL | `docker.io/mysql:8.4.0` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/mysql:8.4.0` |
| Redis Stack / RedisBloom | `docker.io/redis/redis-stack-server:7.4.0-v5` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/redis/redis-stack-server:7.4.0-v5` |
| Consul | `docker.io/hashicorp/consul:1.20.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/hashicorp/consul:1.20.1` |
| Kafka | `docker.io/bitnami/kafka:3.7` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/kafka:3.7` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.4.2` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/ghcr.io/kafbat/kafka-ui:v1.4.2` |
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.15.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.elastic.co/elasticsearch/elasticsearch:8.15.1` |
| Kibana | `docker.elastic.co/kibana/kibana:8.15.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.elastic.co/kibana/kibana:8.15.1` |

说明：Redis 使用 `7.4.0-v5`，Elasticsearch/Kibana 使用 `8.15.1`，是因为这些版本在 `docker.aityp.com` 可直接搜索到对应镜像站地址。Elasticsearch 与 Kibana 保持同一小版本，减少本地兼容性问题。

### 已完成工作

- 已初始化 Git 仓库并推送到 GitHub：`https://github.com/weiyi-ch/Comment-Project.git`
- 已配置 GitHub 仓库描述、topics、分支 ruleset 和合并后自动删除分支。
- 已补充根目录 `LICENSE`、贡献说明、安全说明、PR 模板和 Issue 模板。
- 已新增实现路线文档：`docs/implementation-roadmap.md`
- 已拆分三端 BFF：`comment-student`、`comment-tutor`、`comment-operator`。
- 已定义学生端、助教端、运营端和搜索相关 proto 接口。
- 已实现学生端帖子详情、评论列表、发表评论、删除评论、点赞、取消点赞、我的评论、搜索等入口。
- 已实现助教端发帖、更新帖子、删除帖子、评论列表、评论详情、删除评论、回复评论、删除回复等领域逻辑。
- 已实现运营端待审核列表、审核详情、评论审核、运营视角详情查询等领域逻辑。
- 帖子详情已做 Core/Stats 拆分缓存，评论列表已采用“列表 ID 缓存 + 评论对象缓存”的两级缓存结构。
- 读路径使用 Cache-Aside、singleflight、Redis MGET、MySQL IN 查询减少重复回源。
- 已引入 RedisBloom，用于按 ID 查询时拦截明显不存在的帖子和评论。
- 点赞关系事实数据同步写入 MySQL，保证用户是否点赞的正确性。
- 点赞数和评论数通过 Redis delta 聚合，再由 `comment-task` 异步批量落库。
- MySQL 作为事实源，Elasticsearch 作为搜索读模型，`comment-task` 负责 Canal/Kafka 到 ES 的同步、重试和 DLQ。
- 三端 BFF 已新增注册、登录、刷新 token 和登出接口，密码使用 bcrypt 哈希保存，不再保存明文密码。
- 注册、登录、刷新 token 和登出由 BFF 调用 `comment-service` 的 AuthService 完成，账号与 refresh token session 落 MySQL。
- access token 使用短有效期签名 token，携带 `user_id`、`role`、`token_id`。
- refresh token 使用服务端存储的哈希值，支持轮换、撤销和登出。
- 三端 `internal/auth` 已不再信任 `x-user-id`，只从 Bearer access token 解析可信身份。
- BFF 调用 `comment-service` 时会通过 gRPC metadata 传递可信 `user_id`、`role`、`token_id`，`comment-service` 会写入请求 context。
- 学生端已实现 Redis + Lua 多维令牌桶限流，支持 user、IP、device、API 四类维度一次性原子检查与扣减。

### 待办清单

#### P0：令牌桶与多维限流

- [x] 学生端增加基础令牌桶限流中间件。
- [x] 学生端实现 Redis + Lua 分布式令牌桶，保证多实例下扣减原子性。
- [x] 学生端支持用户、IP、设备、接口四类维度限流。
- [ ] 将多维令牌桶限流扩展到助教端和运营端。
- [ ] 视业务风险增加 `post_id`、评论 ID、搜索关键词等资源维度限流。
- [ ] 先实现单机内存令牌桶，作为 Redis 不可用时的本地降级策略。
- [ ] 登录接口增加 IP + 账号维度限流，防止爆破。
- [ ] 点赞和评论接口增加 user_id + post_id 维度限流，保护热点帖子。
- [ ] 搜索接口增加 user_id/IP + keyword 维度限流，保护 ES。
- [ ] 返回 `429 Too Many Requests`、`Retry-After` 和限流剩余额度响应头。

#### P1：补齐复习笔记中描述但还需增强的能力

- [ ] 继续完善 `comment-tutor` 和 `comment-operator` 的审计能力。
- [ ] 增加敏感词或机器审核模块。
- [ ] 增加审核记录/操作审计表。
- [ ] 增加 ES mapping 初始化脚本和全量重建脚本。
- [ ] 增加 MySQL 与 ES 定期对账任务。
- [ ] 增加 RedisBloom 预热和重建任务。
- [ ] 增加 Redis、Kafka、ES、MySQL 故障下的统一限流、降级、重试和补偿策略。
- [ ] 标准化压测脚本和一致性校验脚本。

#### P2：工程质量

- [x] 增加 token、refresh token、登出、撤销相关单元测试。
- [ ] 增加令牌桶、多维限流、Redis Lua 的单元测试。
- [ ] 增加审核状态流转、点赞幂等、异步计数恢复测试。
- [ ] 增加 BFF 到 `comment-service` 的 metadata 传递集成测试。
- [ ] 补充本地启动说明和依赖中间件说明。

### 后续实现顺序

1. 先做令牌桶和多维限流，用登录、点赞、评论、搜索四类接口验证不同维度的限流效果。
2. 然后补齐自动审核、审计日志、ES 重建/对账、Bloom 预热等复习笔记中的完整设计。
3. 最后完善测试、压测和 README 启动文档，让项目既能运行，也能解释清楚每个机制为什么存在。

更详细的实现路线见：[docs/implementation-roadmap.md](docs/implementation-roadmap.md)。

## English README

### Background

Comment-Project is a Go Kratos based backend review project for a learning community comment system. The original business context has been anonymized. The system serves three roles: students, tutors, and operators. Students read posts, create comments, like content, and search; tutors publish posts, reply to comments, and manage interactions under their own posts; operators review comments, handle violations, and query moderation records.

### Design Goals

This project is designed as more than a CRUD demo. It focuses on backend mechanisms that are commonly discussed in interviews:

- High-concurrency reads: reduce repeated MySQL reads for post detail pages, comment lists, and search results.
- Hotspot writes: avoid frequently updating the same counter rows for likes and comments.
- Data consistency: keep comment moderation, deletion, like relationships, asynchronous counters, and Elasticsearch synchronization eventually correct.
- Authorization boundary: BFF services verify user identity, while the core comment service keeps resource-level authorization.
- Recoverability: handle retries, compensation, reconciliation, and DLQ workflows when Kafka, Redis, Elasticsearch, MySQL, or background workers fail.

### Modules

| Module | Description |
| --- | --- |
| `comment-service` | Core comment-domain service for posts, comments, replies, likes, moderation, and search |
| `comment-task` | Background task service for Kafka consumption, Elasticsearch sync, async counter flushing, retry, and DLQ |
| `comment-student` | Student HTTP BFF for registration, login, authorization, request assembly, and gRPC calls to `comment-service` |
| `comment-tutor` | Tutor HTTP BFF for registration, login, authorization, post/comment/reply/search entry points |
| `comment-operator` | Operator HTTP BFF for registration, login, authorization, moderation lists, moderation actions, and operator search |
| `docs` | Design notes, workflow analysis, load test notes, and implementation roadmap |
| `scripts` | Helper scripts such as mTLS certificate generation |

### Tech Stack

- Go + Kratos
- gRPC / HTTP
- Wire dependency injection
- MySQL / GORM / Gen
- Redis / RedisBloom / Lua
- Kafka
- Elasticsearch
- Canal binlog synchronization
- OpenTelemetry / tracing
- k6 load testing scripts

### One-Command Local Dependency Startup

The repository root provides `docker-compose.yml` for local middleware dependencies, so the project no longer depends on remote MySQL/Redis instances. All Docker images prefer mirror addresses found through `docker.aityp.com`, which makes local startup less dependent on direct access to overseas registries.

Prerequisite: Docker Desktop or Docker Engine is installed and the `docker compose` command is available.

Start everything with one command:

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

Stop local dependencies:

```bash
docker compose down
```

Reset local data and reinitialize MySQL tables:

```bash
docker compose down -v
docker compose up -d
```

Local component endpoints:

| Component | Endpoint |
| --- | --- |
| MySQL | `127.0.0.1:3306`, user `root`, password `root1234`, database `comment` |
| Redis Stack / RedisBloom | `127.0.0.1:6379` |
| Consul | `127.0.0.1:8500` |
| Kafka | `127.0.0.1:9092` |
| Kafka UI | `http://127.0.0.1:8090` |
| Elasticsearch | `http://127.0.0.1:9200` |
| Kibana | `http://127.0.0.1:5601` |

MySQL runs `comment-service/sql/comment.sql` on first startup to initialize tables. If an old volume already exists, decide whether to keep or recreate the data first.

Images used by the current `docker-compose.yml`:

| Component | Source image | docker.aityp mirror image |
| --- | --- | --- |
| MySQL | `docker.io/mysql:8.4.0` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/mysql:8.4.0` |
| Redis Stack / RedisBloom | `docker.io/redis/redis-stack-server:7.4.0-v5` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/redis/redis-stack-server:7.4.0-v5` |
| Consul | `docker.io/hashicorp/consul:1.20.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/hashicorp/consul:1.20.1` |
| Kafka | `docker.io/bitnami/kafka:3.7` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/bitnami/kafka:3.7` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.4.2` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/ghcr.io/kafbat/kafka-ui:v1.4.2` |
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.15.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.elastic.co/elasticsearch/elasticsearch:8.15.1` |
| Kibana | `docker.elastic.co/kibana/kibana:8.15.1` | `swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.elastic.co/kibana/kibana:8.15.1` |

Note: Redis uses `7.4.0-v5`, and Elasticsearch/Kibana use `8.15.1`, because these versions have directly searchable mirror addresses on `docker.aityp.com`. Elasticsearch and Kibana keep the same minor version to reduce local compatibility issues.

### Completed Work

- Initialized the Git repository and pushed it to GitHub: `https://github.com/weiyi-ch/Comment-Project.git`
- Completed GitHub repository description, topics, branch ruleset, and automatic branch deletion after merge.
- Added root `LICENSE`, contribution guide, security policy, pull request template, and issue templates.
- Added the implementation roadmap: `docs/implementation-roadmap.md`
- Split the role-specific BFF services into `comment-student`, `comment-tutor`, and `comment-operator`.
- Defined proto APIs for student, tutor, operator, and search workflows.
- Implemented student-side entry points for post detail, comment list, comment creation/deletion, like/unlike, personal comments, and search.
- Implemented tutor-side domain logic for creating, updating, deleting posts, listing comments, viewing comment detail, deleting comments, replying, and deleting replies.
- Implemented operator-side domain logic for pending moderation lists, moderation detail, comment review, and operator-view detail queries.
- Added Core/Stats split caching for post details and two-level list/object caching for comment lists.
- Added Cache-Aside, singleflight, Redis MGET, and MySQL IN query patterns to reduce duplicate database fallback.
- Added RedisBloom to reject clearly nonexistent post/comment IDs before database access.
- Stored like relationship facts in MySQL to keep user-like state correct.
- Aggregated like/comment counter deltas in Redis and flush them asynchronously through `comment-task`.
- Used MySQL as the source of truth and Elasticsearch as the search read model, with Canal/Kafka synchronization, retry, and DLQ handling.
- Added registration, login, token refresh, and logout endpoints for all three BFF services. Passwords are stored with bcrypt hashes and never stored as plaintext.
- Registration, login, token refresh, and logout are forwarded from BFF services to `comment-service` AuthService, where user accounts and refresh token sessions are persisted in MySQL.
- Added short-lived signed access tokens containing `user_id`, `role`, and `token_id`.
- Added server-side refresh token storage by token hash, with refresh token rotation, revocation, and logout support.
- Updated all three `internal/auth` packages so they no longer trust `x-user-id`; identity is parsed only from Bearer access tokens.
- Forwarded trusted `user_id`, `role`, and `token_id` from BFF services to `comment-service` through gRPC metadata and stored it in the service request context.
- Implemented Redis + Lua multi-dimensional token bucket rate limiting in the student BFF, with atomic check-and-consume across user, IP, device, and API dimensions.

### TODO

#### P0: Token Bucket and Multi-Dimensional Rate Limiting

- [x] Add a basic token bucket rate-limiting middleware to the student BFF.
- [x] Implement a Redis + Lua distributed token bucket in the student BFF so multi-instance token consumption is atomic.
- [x] Support user, IP, device, and API rate-limiting dimensions in the student BFF.
- [ ] Extend the multi-dimensional token bucket limiter to tutor and operator BFF services.
- [ ] Add resource-level dimensions such as `post_id`, comment ID, and search keyword where business risk requires them.
- [ ] Implement an in-memory token bucket as a local fallback strategy when Redis is unavailable.
- [ ] Add IP + account rate limiting for login to reduce brute-force risk.
- [ ] Add user + `post_id` rate limiting for like/comment APIs to protect hot posts.
- [ ] Add user/IP + keyword rate limiting for search APIs to protect Elasticsearch.
- [ ] Return `429 Too Many Requests`, `Retry-After`, and remaining quota headers.

#### P1: Remaining Features From Review Notes

- [ ] Continue improving audit capabilities in `comment-tutor` and `comment-operator`.
- [ ] Add sensitive-word or machine-review modules.
- [ ] Add moderation operation records and audit tables.
- [ ] Add Elasticsearch mapping initialization and full-rebuild scripts.
- [ ] Add scheduled MySQL and Elasticsearch reconciliation tasks.
- [ ] Add RedisBloom warm-up and rebuild tasks.
- [ ] Add unified rate limiting, fallback, retry, and compensation strategies for Redis, Kafka, Elasticsearch, and MySQL failures.
- [ ] Standardize load testing scripts and consistency verification scripts.

#### P2: Engineering Quality

- [x] Add unit tests for access tokens, refresh tokens, logout, and revocation.
- [ ] Add unit tests for token bucket, multi-dimensional rate limiting, and Redis Lua scripts.
- [ ] Add tests for moderation state transitions, like idempotency, and async counter recovery.
- [ ] Add integration tests for BFF-to-`comment-service` metadata propagation.
- [ ] Add local startup instructions and middleware dependency notes.

### Suggested Implementation Order

1. Implement token bucket and multi-dimensional rate limiting, then validate it through login, like, comment, and search APIs.
2. Complete auto-review, audit logs, Elasticsearch rebuild/reconciliation, and RedisBloom warm-up workflows from the review notes.
3. Improve tests, load testing scripts, and README startup documentation so the project can both run and be explained clearly in interviews.

See the detailed roadmap: [docs/implementation-roadmap.md](docs/implementation-roadmap.md).
