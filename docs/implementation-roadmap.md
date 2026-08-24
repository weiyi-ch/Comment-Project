# Comment-Project 实现记录与后续功能清单

## 1. 当前已完成工作记录

### 1.1 Git 与 GitHub 版本记录

- 已在 `/Users/yaowy/GolandProjects/comment` 初始化 Git 仓库。
- 已补充根目录 `.gitignore`，排除 `.idea/`、`.DS_Store`、本地 `configs/config.yaml`、`.gocache/`、mTLS 证书和私钥等不应进入仓库的文件。
- 已创建初始项目快照提交：
  - `c773422 chore: initial project snapshot`
- 已新增根目录 `README.md`：
  - `1b98b23 docs: add project readme`
- 已绑定并推送到 GitHub：
  - `https://github.com/weiyi-ch/Comment-Project.git`
- 当前本地 `main` 已跟踪远端 `origin/main`。

### 1.2 当前代码已有的核心能力

根据当前代码与 `/Users/yaowy/Documents/项目复习笔记/乐学空间` 中的复习笔记对照，项目已经具备以下主体能力：

- `comment-service` 已提供学生端、助教端、运营端和搜索相关 gRPC/HTTP 接口定义。
- `comment-student` 已实现学生端 HTTP 接入层，并通过 gRPC client 调用 `comment-service`。
- `comment-tutor` 已新增助教端 HTTP 接入层，并通过 gRPC client 调用 `comment-service`。
- `comment-operator` 已新增运营端 HTTP 接入层，并通过 gRPC client 调用 `comment-service`。
- 帖子详情读链路已经具备 Core/Stats 拆分缓存、Redis Cache-Aside、singleflight 和统计 delta 叠加。
- 评论列表已采用列表 ID 缓存 + 评论对象缓存的两级结构，并支持批量 MGET 与 MySQL IN 回源。
- RedisBloom 相关代码已出现，按 post/comment ID 做缓存穿透拦截，并在 RedisBloom 不可用时放行。
- 点赞数与评论数已有 Redis delta、dirty set/zset、processing、batch_id、`counter_flush_log` 幂等落库机制。
- `comment-task` 已实现 Canal/Kafka 消息消费、ES 同步、binlog 位点版本判断、retry table、DLQ table 和 retry worker。
- 学生端 BFF 已有基础鉴权过滤器，但目前只是从 `x-user-id` 或简化的 `Bearer student:{id}` 中解析用户 ID，不是真实登录体系。

## 2. 需要优先实现的功能清单

### P0：真实用户登录注册与 Token 体系

- [ ] 增加用户服务或用户模块，支持学生、助教、运营三类账号的基础数据模型。
- [ ] 增加用户注册接口：用户名/手机号/邮箱、密码哈希、角色、状态、创建时间。
- [ ] 增加用户登录接口：校验密码后签发 access token 和 refresh token。
- [ ] 使用安全密码哈希算法，例如 bcrypt 或 argon2，禁止明文保存密码。
- [ ] 设计 access token：
  - 短有效期。
  - 携带 user_id、role、token_id、过期时间。
  - BFF middleware 校验签名和过期时间。
- [ ] 设计 refresh token：
  - 长有效期。
  - 服务端保存 token 哈希、过期时间、是否撤销、设备信息。
  - 刷新时轮换 refresh token，旧 refresh token 失效，降低泄露风险。
- [ ] 增加登出接口：
  - 删除或撤销 refresh token。
  - 可选维护 access token 黑名单，处理强制退出或封禁。
- [ ] 改造三端 `internal/auth`：
  - 不再信任 `x-user-id`。
  - 从 JWT 或 opaque token 中解析可信身份。
  - 把 user_id、role 注入 context。
- [x] 扩展助教端和运营端 BFF 基础鉴权：
  - 学生端只允许学生角色。
  - 助教端只允许助教角色。
  - 运营端只允许运营角色。
- [ ] 在 `comment-service` 保留资源级权限校验：
  - 学生只能删除自己的评论。
  - 助教只能操作自己帖子下的回复或评论。
  - 运营审核必须满足审核状态合法流转。

### P0：限流体系

- [ ] 增加基础令牌桶限流中间件。
- [ ] 支持单机内存令牌桶：
  - 用于本地开发和低成本验证。
  - 适合单实例，不保证多实例全局限流。
- [ ] 支持 Redis 分布式令牌桶：
  - 使用 Lua 脚本保证“计算补充令牌 + 扣减令牌”原子执行。
  - key 中包含限流维度，避免多实例并发超卖令牌。
- [ ] 支持多维令牌桶限流：
  - 全局维度：保护服务总入口。
  - IP 维度：限制异常来源。
  - 用户维度：限制单个 user_id 高频操作。
  - 接口维度：给点赞、评论、搜索、登录等不同接口设置不同阈值。
  - 业务资源维度：按 post_id 限制热点帖子点赞/评论写入。
- [ ] 支持组合限流决策：
  - 一个请求必须同时通过全局、用户、接口、资源等多个桶。
  - 任意桶拒绝则返回 `429 Too Many Requests`。
- [ ] 给不同接口设置初始策略：
  - 登录：IP + 账号维度，防爆破。
  - 注册：IP 维度，防批量注册。
  - 点赞/取消点赞：user_id + post_id 维度，防热点写放大。
  - 发表评论：user_id + post_id 维度，防刷屏。
  - 搜索：user_id/IP + keyword 维度，防高频 ES 查询。
- [ ] 增加限流响应头：
  - `X-RateLimit-Limit`
  - `X-RateLimit-Remaining`
  - `Retry-After`
- [ ] 增加限流观测指标：
  - pass/reject 次数。
  - 按接口、用户、IP、资源维度统计拒绝原因。

### P1：复习笔记中提到但需要继续补强的功能

- [x] 新增 `comment-tutor` 和 `comment-operator` 基础微服务。
  - 当前仓库已包含 `comment-student`、`comment-tutor`、`comment-operator` 三端入口服务。
  - 后续应让三端 BFF 都只负责入口鉴权、参数组装和场景编排，领域规则仍沉到 `comment-service`。
- [ ] 增加统一 auth/user context 传递机制。
  - BFF 从 token 得到身份。
  - gRPC metadata 传递 user_id、role、trace_id。
  - `comment-service` 从可信 metadata/context 中读取调用方身份，而不是依赖客户端自由填写身份字段。
- [ ] 增加敏感词或机器审核模块。
  - 当前复习笔记描述“发表评论后经过机器审核/运营审核”。
  - 代码中已有审核状态和运营审核链路，但还需要明确自动审核入口、审核规则和失败兜底。
- [ ] 增加审核记录或操作审计表。
  - 记录 operator_id、comment_id、action、reason、before_status、after_status、created_at。
  - 用于面试解释运营审核可追踪，也用于问题排查。
- [ ] 增加 ES 索引初始化和重建脚本。
  - 创建 mapping。
  - 从 MySQL 全量重建 post/comment 索引。
  - 支持按 updated_at + id 增量补偿。
- [ ] 增加 MySQL 与 ES 对账任务。
  - 定期扫描 MySQL 变更。
  - 对比 ES 中的 sync_binlog_file/sync_binlog_pos 或 updated_at。
  - 发现缺失或旧版本文档后重新投递或直接修复。
- [ ] 增加 RedisBloom 预热/重建任务。
  - RedisBloom 空集合会造成假阴性风险。
  - 启动时应从 MySQL 分页扫描有效 post_id/comment_id 预热。
  - 重建期间可以旁路 Bloom，完成后再切换。
- [ ] 增加限流、熔断、超时和降级的统一策略。
  - Redis 故障时查询 MySQL 需要限流保护。
  - ES 故障时搜索接口可以失败降级或返回缓存兜底。
  - Kafka 故障时在线写链路要定义同步兜底、outbox 或失败返回边界。
- [ ] 增加压测脚本与一致性校验脚本的标准化入口。
  - 一键压测帖子详情、搜索、点赞、评论。
  - 一键校验 MySQL 计数字段与事实表是否一致。
  - 一键查看 retry/DLQ/processing 是否清空。

### P1：异步计数链路继续验证

- [ ] 给 `counter_flush_log` 增加正式迁移 SQL，而不是只在任务启动时 `CREATE TABLE IF NOT EXISTS`。
- [ ] 增加 processing 长时间未清理的扫描和告警。
- [ ] 增加 Redis delta 丢失后的 MySQL 事实表重算任务。
- [ ] 明确点赞关系事实表与 `post.like_count` 的一致性校验 SQL。
- [ ] 明确可见评论数与 `post.comment_count` 的一致性校验 SQL。
- [ ] 增加任务崩溃恢复测试：
  - claim delta 后进程退出。
  - MySQL 更新失败。
  - Redis 删除 processing 失败。
  - Kafka dirty 通知丢失。

### P2：工程质量与面试可解释性

- [ ] 增加单元测试：
  - token 签发/刷新/撤销。
  - 令牌桶 refill 和扣减。
  - 多维限流组合决策。
  - 审核状态流转。
  - 点赞幂等。
- [ ] 增加集成测试：
  - BFF 鉴权到 comment-service metadata 传递。
  - Redis 缓存 miss/hit。
  - ES retry/DLQ。
  - counter processing 恢复。
- [ ] 增加 README 中的启动说明。
  - MySQL、Redis、Kafka、ES、Canal 启动方式。
  - 各服务启动命令。
  - 必要配置项说明。
- [ ] 增加架构图或时序图：
  - 登录鉴权链路。
  - 多维限流链路。
  - 评论发布与审核链路。
  - ES 同步链路。
  - 异步计数链路。

## 3. 建议实现顺序

### 第一阶段：入口安全基建

1. 先实现用户注册、登录、JWT access token、refresh token。
2. 改造三端 BFF 鉴权，不再信任 `x-user-id`。
3. 补充 token 单元测试和登录接口文档。

原因：后续限流、权限和审计都依赖可信 user_id/role。如果身份来源不可信，所有“按用户限流”和“资源归属校验”的解释都会站不稳。

### 第二阶段：令牌桶与多维限流

1. 先做单机令牌桶，验证接口和测试。
2. 再做 Redis + Lua 分布式令牌桶。
3. 最后接入多维组合策略。

原因：限流不是简单限制 QPS，而是保护不同瓶颈。登录保护账号安全，点赞/评论保护热点写，搜索保护 ES，Redis 故障时保护 MySQL。

### 第三阶段：补齐复习笔记与代码差距

1. 完善 comment-tutor/comment-operator。
2. 补自动审核、审计日志。
3. 补 ES mapping、重建、对账。
4. 补 Bloom 预热与重建。
5. 补压测与一致性校验脚本。

原因：这些功能能把复习笔记中的“设计描述”变成可运行、可演示、可面试追问的实现。

## 4. 面试解释重点

后续每实现一个功能，都要同时准备三层材料：

- 代码实现：接口、数据表、中间件、核心函数、测试。
- 机制解释：解决什么问题，为什么简单方案不够，内部如何保证正确性。
- 失败边界：超时、重试、幂等、降级、补偿、监控如何处理。

特别是限流和 token 体系，不能只说“提升安全性”或“保护系统”。要解释清楚：

- 哪些请求被限制。
- 用哪个维度限制。
- Redis Lua 如何保证分布式扣减原子性。
- access token 和 refresh token 生命周期如何分离。
- refresh token 泄露、重复使用、用户登出、账号封禁时怎么处理。
- 这些机制如何和乐学空间的评论、点赞、搜索热点场景关联。
