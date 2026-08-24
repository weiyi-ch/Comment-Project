# Contributing / 贡献说明

## 中文

这个仓库主要用于学习社区评论系统的项目复盘和面试表达沉淀。提交代码时，请尽量让每次变更围绕一个明确目标，例如鉴权、限流、缓存、异步计数、ES 同步或审核流程。

### 分支约定

- 功能分支：`feature/<topic>`
- 修复分支：`fix/<topic>`
- 文档分支：`docs/<topic>` 或 `chore/<topic>`

### 提交前检查

- 修改 Go 代码后运行对应模块的 `go test ./...`。
- 修改 README 或项目说明时，保持中文和英文双版同步。
- 不提交本地配置、密钥、证书私钥、运行时数据和日志文件。
- PR 描述需要说明业务背景、设计原因、核心机制、验证方式和剩余风险。

### 代码风格

- 优先复用当前项目已有结构和命名。
- 业务逻辑要有明确边界：BFF 负责入口认证和请求组装，`comment-service` 负责领域规则和资源级权限校验。
- 涉及并发、一致性、重试、补偿、限流、幂等的代码，需要在 PR 中说明机制。

## English

This repository is mainly used to review and explain a learning-community comment system. Each change should focus on one clear goal, such as authentication, rate limiting, caching, async counters, Elasticsearch synchronization, or moderation workflows.

### Branch Naming

- Feature branches: `feature/<topic>`
- Fix branches: `fix/<topic>`
- Documentation branches: `docs/<topic>` or `chore/<topic>`

### Before Submitting

- Run `go test ./...` in the affected Go module after changing Go code.
- Keep Chinese and English README/project documentation in sync.
- Do not commit local configs, secrets, private keys, runtime data, or logs.
- PR descriptions should explain the business context, design reason, core mechanism, verification method, and remaining risks.

### Code Style

- Prefer existing project structure and naming conventions.
- Keep responsibility boundaries clear: BFF services handle entry authentication and request assembly, while `comment-service` handles domain rules and resource-level authorization.
- For concurrency, consistency, retry, compensation, rate limiting, or idempotency logic, explain the mechanism in the PR.
