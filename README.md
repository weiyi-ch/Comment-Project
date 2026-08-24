# Comment-Project

一个基于 Go Kratos 的学习社区评论系统复盘项目，用于沉淀评论、点赞、审核、搜索、缓存、异步计数和微服务调用链路等后端设计能力。

项目背景来自某在线教育/学习社区业务场景，具体公司与业务名称已做泛化处理。系统面向学生、助教、运营三类角色：学生可以浏览知识帖、发表评论、点赞和搜索；助教可以发布帖子、回复评论和管理自己帖子下的互动；运营可以审核评论、处理违规内容并查询审核列表。

## 项目目标

这个项目不是只做 CRUD，而是围绕评论系统中最容易被面试追问的几个问题做工程化实现：

- 高并发读：帖子详情、评论列表、搜索结果如何减少 MySQL 回源。
- 热点写：点赞数、评论数如何避免频繁更新同一行造成锁竞争。
- 数据一致性：评论审核、删除、点赞关系、异步计数和 ES 同步如何保证最终正确。
- 权限边界：BFF 做身份认证，核心服务做资源级权限校验。
- 可恢复性：Kafka、Redis、ES、MySQL 或后台任务异常时如何重试、补偿和对账。

## 系统模块

| 模块 | 说明 |
| --- | --- |
| `comment-service` | 评论领域核心服务，负责帖子、评论、回复、点赞、审核、搜索等主要业务逻辑 |
| `comment-task` | 异步任务服务，负责消费 Kafka、同步 ES、异步刷点赞/评论计数、处理 retry/DLQ |
| `student-bff` | 学生端 HTTP BFF，负责学生端入口鉴权、参数组装，并通过 gRPC 调用 `comment-service` |
| `docs` | 项目设计、链路分析、压测记录、后续实现清单 |
| `scripts` | 辅助脚本，例如 mTLS 证书生成 |

## 技术栈

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

## 已完成工作

### 项目版本管理

- 已初始化 Git 仓库。
- 已配置 `.gitignore`，排除 IDE 文件、本地配置、缓存目录、mTLS 私钥和证书生成物。
- 已推送到 GitHub：`https://github.com/weiyi-ch/Comment-Project.git`
- 已新增实现路线文档：`docs/implementation-roadmap.md`

### 核心业务接口

- 已定义学生端、助教端、运营端和搜索相关 proto 接口。
- 已实现学生端帖子详情、评论列表、发表评论、删除评论、点赞、取消点赞、我的评论、搜索等入口。
- 已实现助教端发帖、更新帖子、删除帖子、评论列表、评论详情、删除评论、回复评论、删除回复等领域逻辑。
- 已实现运营端待审核列表、审核详情、评论审核、运营视角详情查询等领域逻辑。

### 缓存与查询优化

- 帖子详情已做 Core/Stats 拆分缓存。
- 评论列表已采用“列表 ID 缓存 + 评论对象缓存”的两级缓存结构。
- 读路径使用 Cache-Aside、singleflight、Redis MGET、MySQL IN 查询减少重复回源。
- 已引入 RedisBloom，用于按 ID 查询时拦截明显不存在的帖子和评论。
- 缓存 TTL 带抖动，降低同类 key 集中过期风险。

### 热点写与异步计数

- 点赞关系事实数据同步写入 MySQL，保证用户是否点赞的正确性。
- 点赞数和评论数通过 Redis delta 聚合，再由 `comment-task` 异步批量落库。
- 已设计 dirty set/zset、processing、batch_id、`counter_flush_log`，降低任务崩溃或重复执行导致的计数错误。
- 读侧会将 MySQL 基础计数与 Redis pending/processing delta 叠加，减少异步落库带来的展示延迟。

### CQRS 与 ES 同步

- MySQL 作为评论、帖子、点赞关系和审核状态的事实源。
- Elasticsearch 作为搜索读模型，承接帖子和评论关键词检索。
- `comment-task` 消费 Canal/Kafka 消息，根据 MySQL 最新数据构建 ES 文档。
- 已加入 binlog file/pos 版本字段，避免旧消息覆盖新文档。
- 已实现 ES retry table、DLQ table 和 retry worker，避免同步失败后永久丢失。

### BFF 与鉴权边界

- `student-bff` 已有基础 HTTP 接入层和学生身份解析。
- 当前身份解析仍是简化实现，支持 `x-user-id` 或 `Bearer student:{id}`，还不是真实 token 体系。
- 核心服务侧已保留部分资源级权限校验，例如学生只能删除自己的评论，助教只能操作自己帖子下的内容，运营审核需要满足状态流转。

## 待办清单

### P0：用户注册登录与 Token 体系

- [ ] 增加用户数据模型，支持学生、助教、运营三类角色。
- [ ] 增加注册接口，保存密码哈希，禁止明文密码。
- [ ] 增加登录接口，签发 access token 和 refresh token。
- [ ] 实现 access token 短有效期校验，携带 user_id、role、token_id。
- [ ] 实现 refresh token 服务端存储、轮换、撤销和登出。
- [ ] 改造 `student-bff/internal/auth`，不再信任 `x-user-id`。
- [ ] 增加助教端和运营端 BFF 鉴权。
- [ ] 将可信身份通过 gRPC metadata/context 传递到 `comment-service`。

### P0：令牌桶与多维限流

- [ ] 增加基础令牌桶限流中间件。
- [ ] 先实现单机内存令牌桶，验证 refill 和扣减逻辑。
- [ ] 再实现 Redis + Lua 分布式令牌桶，保证多实例下扣减原子性。
- [ ] 支持全局、IP、用户、接口、post_id 等多维限流。
- [ ] 登录接口增加 IP + 账号维度限流，防止爆破。
- [ ] 点赞和评论接口增加 user_id + post_id 维度限流，保护热点帖子。
- [ ] 搜索接口增加 user_id/IP + keyword 维度限流，保护 ES。
- [ ] 返回 `429 Too Many Requests`、`Retry-After` 和限流剩余额度响应头。

### P1：补齐复习笔记中描述但还需增强的能力

- [ ] 完整补齐 `tutor-bff` 和 `operator-bff`。
- [ ] 增加敏感词或机器审核模块。
- [ ] 增加审核记录/操作审计表。
- [ ] 增加 ES mapping 初始化脚本和全量重建脚本。
- [ ] 增加 MySQL 与 ES 定期对账任务。
- [ ] 增加 RedisBloom 预热和重建任务。
- [ ] 增加 Redis、Kafka、ES、MySQL 故障下的统一限流、降级、重试和补偿策略。
- [ ] 标准化压测脚本和一致性校验脚本。

### P2：工程质量

- [ ] 增加 token、refresh token、登出、撤销相关单元测试。
- [ ] 增加令牌桶、多维限流、Redis Lua 的单元测试。
- [ ] 增加审核状态流转、点赞幂等、异步计数恢复测试。
- [ ] 增加 BFF 到 `comment-service` 的 metadata 传递集成测试。
- [ ] 补充本地启动说明和依赖中间件说明。

## 后续实现顺序

1. 先做真实登录注册和 token 体系，因为后续权限、限流、审计都依赖可信 `user_id/role`。
2. 再做令牌桶和多维限流，用登录、点赞、评论、搜索四类接口验证不同维度的限流效果。
3. 然后补齐三端 BFF、自动审核、审计日志、ES 重建/对账、Bloom 预热等复习笔记中的完整设计。
4. 最后完善测试、压测和 README 启动文档，让项目既能运行，也能解释清楚每个机制为什么存在。

更详细的实现路线见：[docs/implementation-roadmap.md](docs/implementation-roadmap.md)。
