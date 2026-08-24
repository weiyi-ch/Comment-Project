# comment-service 文档索引

当前 `docs` 已按复习和面试使用方式重新收敛，避免同一内容散落在多份文件里。

## 推荐阅读顺序

1. [项目设计与面试说明](project-design-and-interview.md)

   从项目背景、技术选型、表结构、核心功能、缓存、ES、并发和异常处理讲清楚“为什么这样做”。适合准备面试自述。

2. [缓存设计说明](cache-design.md)

   专门解释 Redis 缓存分层、评论列表为什么拆成 ID 列表 + 评论对象、三端如何复用对象缓存、TTL 抖动和一致性边界。

3. [全接口数据流向报告](data-flow-tracing-report.md)

   按学生、助教、运营三个角色梳理全部接口的数据流向。适合回答“某个接口从 Controller 到 DB 怎么走”。

4. [帖子点赞计数异步落库设计](post-like-async-counter-design.md)

   专门解释热点帖子点赞/取消点赞如何从同步更新 `post.like_count` 改成 Redis delta 去重、`dirty_at` 安静窗口、Kafka `postlike` dirty 通知和 `comment-task` 后台批量落库，并逐步列出核心代码。

5. [压测方案](load-test-plan.md)

   重新设计后的压测入口，按 Smoke、读链路、缓存策略、搜索、并发写、阶梯压测组织。

6. [压测记录](load-test-record.md)

   只记录重新压测后的真实结果。你把 k6 summary、trace 和 SQL 反查发来后，我会追加到这里。

7. [埋点代码流程说明](tracing-code-flow.md)

   专门解释当前 OpenTelemetry / Jaeger 埋点代码怎么初始化、怎么生成 trace、怎么在 `GetPostDetailStudent` 链路上展开 Service、Usecase、Repo、Redis、MySQL span。

## 合并说明

已合并的旧文档：

- `architecture-and-interview.md` -> `project-design-and-interview.md`
- `developer-design-rationale.md` -> `project-design-and-interview.md`
- `refactor-report.md` -> `project-design-and-interview.md`
- 旧压测文档已删除，新的压测内容统一放在 `load-test-plan.md` 和 `load-test-record.md`
