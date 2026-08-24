# Comment-Project

Comment-Project 是一个基于 Go Kratos 的学习社区评论系统复盘项目，业务背景已做匿名化处理。项目聚焦评论、点赞、审核、搜索、缓存、异步计数、身份认证、限流和 MySQL 到 Elasticsearch 同步等后端核心链路，目标是把“能跑的代码”和“能解释清楚的工程设计”放在一起。

This is a Go Kratos based backend review project for a learning-community comment system. The original business context has been anonymized. It focuses on comments, likes, moderation, search, caching, async counters, authentication, rate limiting, and MySQL-to-Elasticsearch synchronization.

## 中文说明

### 项目定位

系统面向三类角色：

- 学生：浏览帖子、评论、点赞、搜索。
- 助教：发布帖子、管理自己帖子下的评论与回复。
- 运营：审核评论、处理违规内容、查询审核记录。

这个项目重点不是普通 CRUD，而是评论系统里常见的工程问题：

- 高并发读：帖子详情和评论列表要减少重复回源 MySQL。
- 热点写：点赞数、评论数不能直接高频更新 `post` 主表。
- 最终一致性：Redis、Kafka、MySQL、ES 任一环节失败后要能重试、补偿和对账。
- 安全边界：BFF 校验登录态，核心服务基于可信身份做资源级权限判断。

### 服务划分

| 模块 | 职责 |
| --- | --- |
| `comment-service` | 评论领域核心服务，负责帖子、评论、回复、点赞、审核、搜索和账号会话 |
| `comment-student` | 学生端 BFF，负责学生端 HTTP 接口、鉴权、限流和 gRPC 调用 |
| `comment-tutor` | 助教端 BFF，负责助教端发帖、评论管理、回复和搜索入口 |
| `comment-operator` | 运营端 BFF，负责审核列表、审核动作和运营查询入口 |
| `comment-task` | 异步任务服务，负责 Kafka 消费、ES 同步、计数落库、重试和 DLQ |
| `docs` | 设计文档、复习笔记、压测记录和实现路线 |

### 核心机制

1. 多级缓存
   - 帖子内容和帖子计数分开缓存，避免点赞/评论数频繁变化导致帖子主体缓存失效。
   - 评论列表缓存 ID 列表，评论对象单独缓存；缺失对象通过 Redis MGET + MySQL IN 批量回源。

2. 异步计数
   - 点赞关系以 `post_like` 作为事实表。
   - 点赞数、评论数拆到 `post_counter`，避免高频计数更新污染 `post` 表 binlog。
   - Redis 聚合 pending delta，后台任务生成 batch，落库后再更新 `post_counter`。
   - 定期校准任务只在 pending、processing 和 batch 都清空时，才用事实表 count 修正 `post_counter`。

3. MySQL 到 Elasticsearch 同步
   - MySQL 是事实源，ES 是搜索读模型。
   - Canal/Kafka 推送变更，`comment-task` 消费后重查 MySQL 构建最新 ES 文档。
   - 删除或源记录不存在时写 tombstone 文档，而不是直接物理删除，避免乱序消息导致旧数据复活。
   - ES 写入失败进入 `es_sync_retry`，多次失败后进入 `es_sync_dlq`。

4. 身份认证与限流
   - 三端 BFF 提供注册、登录、刷新 token 和登出接口。
   - 密码使用 bcrypt 哈希保存，refresh token 服务端存储、轮换、撤销。
   - access token 短有效期，携带 `user_id`、`role`、`token_id`。
   - BFF 不再信任 `x-user-id`，可信身份通过 gRPC metadata 传递到 `comment-service`。
   - 学生端已实现 Redis + Lua 多维令牌桶限流，支持 user、IP、device、API 四类维度原子检查和统一扣减。

### 本地启动

项目根目录提供 `docker-compose.yml`，本地直接启动 MySQL、Redis、Consul、Kafka、Kafka UI、Elasticsearch 和 Kibana。镜像优先使用 `docker.aityp.com` 可搜索到的镜像站地址，减少直接拉取海外镜像失败的问题。

```bash
docker compose up -d
docker compose ps
```

停止：

```bash
docker compose down
```

清空本地 volume 并重新初始化：

```bash
docker compose down -v
docker compose up -d
```

如果只是手动删除了 MySQL 中的 `comment` 库或部分表，需要重新执行初始化 SQL：

```bash
docker compose exec -T mysql mysql -uroot -proot1234 < comment-service/sql/comment.sql
```

| 组件 | 地址 |
| --- | --- |
| MySQL | `127.0.0.1:3306`，账号 `root`，密码 `root1234`，库名 `comment` |
| Redis Stack / RedisBloom | `127.0.0.1:6379` |
| Consul | `127.0.0.1:8500` |
| Kafka | `127.0.0.1:9092` |
| Kafka UI | `http://127.0.0.1:8090` |
| Elasticsearch | `http://127.0.0.1:9200` |
| Kibana | `http://127.0.0.1:5601` |

### 当前进度

- [x] GitHub 仓库初始化、基础文档、Issue/PR 模板、分支规则集。
- [x] 拆分 `comment-student`、`comment-tutor`、`comment-operator` 三端 BFF。
- [x] 三端注册、登录、刷新 token、登出；密码哈希保存，refresh token 支持轮换和撤销。
- [x] 三端 BFF 通过 access token 解析身份，并通过 gRPC metadata 传给 `comment-service`。
- [x] 学生端 Redis + Lua 多维令牌桶限流。
- [x] 帖子 Core/Stats 拆分缓存，评论列表 ID 缓存和评论对象缓存。
- [x] 点赞关系事实表、`post_counter` 计数表、Redis delta 聚合、batch 异步落库。
- [x] 计数校准任务，避免在异步增量未清空时覆盖新数据。
- [x] Canal/Kafka 到 ES 同步链路，支持 tombstone、retry 和 DLQ。

### TODO

- [ ] 将多维令牌桶扩展到助教端和运营端。
- [ ] 给登录、点赞、评论、搜索增加更细的资源维度限流。
- [ ] 增加 Redis 不可用时的本地限流降级。
- [ ] 增加敏感词或机器审核模块。
- [ ] 增加审核操作记录和审计查询。
- [ ] 增加 ES mapping 初始化、索引 alias、全量重建和 MySQL/ES 对账任务。
- [ ] 增加 RedisBloom 预热和重建任务。
- [ ] 完善异步计数、限流、ES 同步、BFF metadata 的单元测试和集成测试。
- [ ] 补充 k6 压测脚本和一致性校验脚本。

更多路线见：[docs/implementation-roadmap.md](docs/implementation-roadmap.md)。

## English README

### Project Focus

The system supports three roles:

- Student: browse posts, create comments, like content, and search.
- Tutor: publish posts, manage comments, and reply under owned posts.
- Operator: review comments, handle violations, and query moderation records.

The project is not a plain CRUD demo. It focuses on backend problems that are important in real systems and interviews:

- High-concurrency reads: reduce repeated MySQL fallback for post details and comment lists.
- Hotspot writes: avoid frequent updates to the `post` table for like/comment counters.
- Eventual consistency: make Redis, Kafka, MySQL, and Elasticsearch workflows recoverable.
- Security boundary: BFF services verify user identity, while the core service handles resource-level authorization.

### Services

| Module | Responsibility |
| --- | --- |
| `comment-service` | Core domain service for posts, comments, replies, likes, moderation, search, and user sessions |
| `comment-student` | Student BFF for HTTP APIs, authentication, rate limiting, and gRPC calls |
| `comment-tutor` | Tutor BFF for post management, comment management, replies, and search entry points |
| `comment-operator` | Operator BFF for moderation lists, review actions, and operator queries |
| `comment-task` | Background worker for Kafka consumption, Elasticsearch sync, counter flushing, retry, and DLQ |
| `docs` | Design notes, review notes, load testing notes, and roadmap |

### Core Mechanisms

1. Multi-level caching
   - Post core data and post counters are cached separately, so frequent counter changes do not invalidate the whole post cache.
   - Comment lists cache ID lists, while comment objects are cached separately. Missing objects are loaded through Redis MGET and MySQL IN batch fallback.

2. Async counters
   - `post_like` is the fact table for like relationships.
   - Like and comment counters are stored in `post_counter`, separated from the `post` table.
   - Redis accumulates pending deltas. Background tasks create batches and flush them into MySQL.
   - Counter reconciliation only overwrites `post_counter` when pending data, processing data, and unfinished batches are all empty.

3. MySQL-to-Elasticsearch synchronization
   - MySQL is the source of truth. Elasticsearch is the search read model.
   - Canal/Kafka publishes changes. `comment-task` consumes events and reloads the latest MySQL row before indexing.
   - Missing or deleted source rows are indexed as tombstone documents instead of being physically deleted, preventing out-of-order events from resurrecting stale documents.
   - Failed ES writes are stored in `es_sync_retry`; permanently failed records are moved to `es_sync_dlq`.

4. Authentication and rate limiting
   - All three BFF services provide registration, login, token refresh, and logout.
   - Passwords are stored as bcrypt hashes. Refresh tokens are stored server-side, rotated, revoked, and invalidated on logout.
   - Short-lived access tokens carry `user_id`, `role`, and `token_id`.
   - BFF services no longer trust `x-user-id`; trusted identity is propagated to `comment-service` through gRPC metadata.
   - The student BFF has Redis + Lua multi-dimensional token bucket rate limiting across user, IP, device, and API dimensions.

### Local Startup

The repository root contains `docker-compose.yml` for local MySQL, Redis, Consul, Kafka, Kafka UI, Elasticsearch, and Kibana. Images prefer mirror addresses discoverable through `docker.aityp.com`.

```bash
docker compose up -d
docker compose ps
```

Stop services:

```bash
docker compose down
```

Reset local data and reinitialize tables:

```bash
docker compose down -v
docker compose up -d
```

If only the MySQL `comment` database or some tables were manually deleted, run:

```bash
docker compose exec -T mysql mysql -uroot -proot1234 < comment-service/sql/comment.sql
```

| Component | Endpoint |
| --- | --- |
| MySQL | `127.0.0.1:3306`, user `root`, password `root1234`, database `comment` |
| Redis Stack / RedisBloom | `127.0.0.1:6379` |
| Consul | `127.0.0.1:8500` |
| Kafka | `127.0.0.1:9092` |
| Kafka UI | `http://127.0.0.1:8090` |
| Elasticsearch | `http://127.0.0.1:9200` |
| Kibana | `http://127.0.0.1:5601` |

### Current Progress

- [x] GitHub repository setup, base docs, Issue/PR templates, and branch ruleset.
- [x] Split BFF services into `comment-student`, `comment-tutor`, and `comment-operator`.
- [x] Registration, login, token refresh, and logout for all three BFF services.
- [x] Password hashing, refresh token rotation/revocation, and short-lived access tokens.
- [x] gRPC metadata propagation from BFF services to `comment-service`.
- [x] Redis + Lua multi-dimensional token bucket rate limiting in the student BFF.
- [x] Post Core/Stats split caching and comment list/object two-level caching.
- [x] Like fact table, `post_counter`, Redis delta aggregation, and batch-based async counter flushing.
- [x] Counter reconciliation that avoids overwriting unflushed async deltas.
- [x] Canal/Kafka-to-ES synchronization with tombstone documents, retry, and DLQ.

### TODO

- [ ] Extend multi-dimensional rate limiting to tutor and operator BFF services.
- [ ] Add resource-level rate limiting for login, like, comment, and search APIs.
- [ ] Add local fallback rate limiting when Redis is unavailable.
- [ ] Add sensitive-word or machine-review support.
- [ ] Add moderation operation records and audit queries.
- [ ] Add ES mapping initialization, index aliases, full rebuild, and MySQL/ES reconciliation.
- [ ] Add RedisBloom warm-up and rebuild tasks.
- [ ] Add unit and integration tests for async counters, rate limiting, ES sync, and BFF metadata.
- [ ] Add k6 load testing scripts and consistency verification scripts.
