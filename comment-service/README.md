# comment-service

`comment-service` 是知识帖评论系统的主服务，基于 Kratos + MySQL + Redis + Elasticsearch 构建，提供学生端、助教端、运营端的帖子、评论、回复、搜索、审核和点赞接口。

## 核心能力

- 学生端：帖子详情、帖子搜索、评论搜索、点赞/取消点赞、发表评论、删除自己的评论、评论详情、评论列表、我的评论。
- 助教端：发帖、编辑/删除帖子、查看自己帖子、查看评论、回复评论、删除回复、删除帖子下评论。
- 运营端：审核评论、查询待审核列表、搜索评论、查看帖子和审核详情。
- 帖子详情链路：帖子 Core/Stats 分离缓存，评论使用列表 ID 缓存 + `mysql:comment:{comment_id}` 对象缓存，读侧叠加 Redis pending/processing 计数 delta。
- 搜索链路：MySQL 通过 Canal/Kafka 同步到 Elasticsearch，`comment-task` 回查 MySQL 最新数据构建 ES 文档，并用同步位点做幂等和乱序控制。
- 热点计数：`post_like` 和评论记录同步保存事实数据，`post.like_count/comment_count` 通过 Redis pending/processing、Kafka dirty 通知和 `comment-task` zset 调度异步聚合落库。

## 文档入口

文档统一放在 [docs/README.md](docs/README.md)。建议优先阅读：

1. [项目设计与面试说明](docs/project-design-and-interview.md)
2. [全接口数据流向报告](docs/data-flow-tracing-report.md)
3. [帖子点赞计数异步落库设计](docs/post-like-async-counter-design.md)
4. [压测方案](docs/load-test-plan.md)
5. [压测记录](docs/load-test-record.md)

## 本地常用命令

```bash
# 生成 proto / wire / 依赖整理
make all

# 只跑编译和测试
go test ./...

# 启动服务
kratos run
```

## 配置说明

- `configs/config.example.yaml` 是示例配置。
- `configs/config.yaml` 是本地运行配置，包含真实环境地址时不要提交到公共仓库。
- MySQL/Redis/Elasticsearch/Kafka 需要和 `comment-task` 保持同一套环境，否则点赞计数异步落库和搜索同步会出现分叉。

## 压测入口

k6 脚本在 [tools/k6/comment-service.js](tools/k6/comment-service.js)。示例：

```bash
BASE_URL=http://127.0.0.1:8888 \
SCENARIO=like-post \
POST_ID=$HOT_POST_ID \
POST_IDS=$HOT_POST_ID \
POST_ID_MODE=round_robin \
STUDENT_START=$STUDENT_START \
VUS=30 \
DURATION=20s \
ALLOW_BUSINESS_ERRORS=true \
SUMMARY_PATH=reports/k6/like-many.json \
k6 run tools/k6/comment-service.js
```

点赞/取消点赞/评论数是异步落库，压测结束后等待安静窗口和调度任务处理完成，再用 SQL 对比 `post.like_count/comment_count` 和事实表聚合计数。
