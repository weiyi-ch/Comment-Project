# comment-service 缓存设计说明

这份文档专门讲缓存。主线是：MySQL 是事实源，Redis 只做热点读加速、短期列表缓存、搜索结果缓存和并发保护。

## 1. 缓存目标

缓存要解决四类问题：

| 问题 | 解决方式 |
| --- | --- |
| 热点详情反复查 MySQL | 帖子、评论、回复对象缓存 |
| 评论列表/审核列表分页重复查询 | 列表 ID 缓存 + 评论对象缓存 |
| 不存在 ID 被反复请求 | Bloom Filter + 空值缓存 |
| 缓存失效瞬间大量回源 | singleflight + TTL 随机抖动 |

## 2. 缓存类型

| 类型 | 示例 key | value | TTL |
| --- | --- | --- | --- |
| 帖子对象 | `mysql:post:{post_id}` | `Post` JSON | 10 分钟 ±10% |
| 评论对象 | `mysql:comment:{comment_id}` | `StudyComment` JSON | 10 分钟 ±10% |
| 回复列表 | `mysql:comment_replies:{comment_id}` | 当前评论的回复数组 | 10 分钟 ±10% |
| 学生评论列表 ID | `mysql:student:post_comments:{post_id}:{hash}` | `version/comment_ids/total` | 2 分钟 ±10% |
| 学生我的评论 ID | `mysql:student:my_comments:{student_id}:{hash}` | `version/comment_ids/total` | 2 分钟 ±10% |
| 助教评论列表 ID | `mysql:post_comments:{post_id}:{hash}` | `version/comment_ids/total` | 2 分钟 ±10% |
| 运营审核列表 ID | `mysql:operator:audit_comments:{audit_status}:{hash}` | `version/comment_ids/total` | 2 分钟 ±10% |
| 运营帖子评论 ID | `mysql:operator:post_comments:{post_id}:{hash}` | `version/comment_ids/total` | 2 分钟 ±10% |
| 搜索结果 | `es:post_search:{hash}` / `es:comment_search:{hash}` | 搜索结果 JSON | 2 分钟 ±10% |
| 空值缓存 | 对象 key -> `__nil__` | 空标记 | 30 秒 ±10% |
| Bloom | `bf:post` / `bf:comment` | RedisBloom | 常驻 |
| 点赞短锁 | `lock:post_like:{post_id}:{student_id}` | token | 3 秒 |

## 3. 评论列表为什么拆成 ID + 对象

旧方案是：

```text
mysql:student:post_comments:{hash(post_id,page_num,page_size)}
  -> 整页评论对象 JSON
```

问题：

- 同一条评论会在帖子详情、评论列表、我的评论、审核列表里重复缓存。
- 修改一条评论时，很难知道它存在于哪些列表 key 中。
- `page_size` 越大，单个 Redis value 越大。

现在改成：

```text
列表 key:
mysql:student:post_comments:{post_id}:{hash}
  -> {"version":2,"comment_ids":[...],"total":100}

对象 key:
mysql:comment:{comment_id}
  -> 单条评论对象 JSON
```

读取流程：

```text
ListPostComments
  -> Redis GET list id cache
  -> hit: 得到 comment_ids 和 total
  -> Redis MGET mysql:comment:{comment_id}
  -> 部分评论对象 miss
  -> MySQL SELECT study_comment WHERE comment_id IN (...)
  -> 回填 mysql:comment:{comment_id}
  -> 按 comment_ids 顺序组装返回
```

好处：

- 列表缓存 value 更小。
- 评论对象可以被三端列表、评论详情、帖子详情复用。
- 单条评论变更时，删除 `mysql:comment:{comment_id}` 即可让对象缓存失效。
- 列表 key 带 `post_id/student_id/audit_status` 前缀，写操作可以按前缀删除相关分页列表。
- TTL 仍保留，用来兜底异常场景和历史 key。

## 4. 三端落地

| 角色 | 列表场景 | 列表 key | 对象读取 |
| --- | --- | --- | --- |
| 学生 | 帖子评论列表 | `mysql:student:post_comments:{post_id}:{hash}` | `redis.MGET mysql:comment:{id}` |
| 学生 | 我的评论列表 | `mysql:student:my_comments:{student_id}:{hash}` | `redis.MGET mysql:comment:{id}` |
| 助教 | 帖子评论列表 | `mysql:post_comments:{post_id}:{hash}` | `redis.MGET mysql:comment:{id}` |
| 运营 | 审核列表 | `mysql:operator:audit_comments:{audit_status}:{hash}` | `redis.MGET mysql:comment:{id}` |
| 运营 | 帖子评论列表 | `mysql:operator:post_comments:{post_id}:{hash}` | `redis.MGET mysql:comment:{id}` |

Jaeger 里三端的对象缓存 span 名都统一成 `redis.MGET mysql:comment`，通过 `app.role=student/tutor/operator` 区分来源角色。这样既能看清真实 Redis key，又不会误以为三端各自存了一份评论对象。

代码位置：

- [student.go](/Users/yaowy/GolandProjects/comment-service/internal/data/student.go:535)
- [tutor.go](/Users/yaowy/GolandProjects/comment-service/internal/data/tutor.go:385)
- [operator.go](/Users/yaowy/GolandProjects/comment-service/internal/data/operator.go:202)

## 5. 写操作如何失效

| 写操作 | 主动删除 | TTL 作用 |
| --- | --- | --- |
| 点赞/取消点赞 | 删除 `mysql:post:{post_id}` | 无 |
| 发表评论 | 删除 `mysql:post:{post_id}`；删除相关帖子评论/我的评论/审核列表 ID 缓存 | 异常兜底 |
| 删除评论 | 删除 `mysql:comment:{comment_id}`；删除 `mysql:post:{post_id}`；删除相关列表 ID 缓存 | 异常兜底 |
| 审核通过 | 删除 `mysql:comment:{comment_id}`；删除待审核/已通过审核列表 ID 缓存 | 异常兜底 |
| 审核驳回 | 删除 `mysql:comment:{comment_id}`；必要时删除 `mysql:post:{post_id}`；删除相关审核/帖子评论列表 ID 缓存 | 异常兜底 |
| 回复评论 | 删除 `mysql:comment_replies:{comment_id}` | 无 |

写操作只删除缓存，不主动写入数据缓存。新增评论不会直接 `SET mysql:comment:{comment_id}`，新增回复也不会直接 `SET mysql:reply:{reply_id}`；只有读请求真正查到 MySQL 数据后，才回填对象缓存和列表缓存。

列表 ID 缓存也只删除，不手动追加、删除、修改列表里的 `comment_id`。原因是评论列表有排序、分页和 total，手动改缓存很容易只改了第一页、漏掉后续页，或者让 total 和实际数据不一致。删除缓存后，下次查询从 MySQL 重新加载，再写入新的 ID 列表。

## 6. 一致性边界

当前缓存是一种“最终一致”设计：

- 对象缓存采用写后删除，读到 MySQL 数据后再回填。
- 列表缓存只存 ID，写操作会主动删除相关列表，TTL 2 分钟作为兜底。
- 评论对象 MGET 时，如果对象已被删除或审核状态变化，会尽量通过对象 miss 回源拿新值。
- 运营审核列表这种变化频繁的场景，仍要接受短 TTL 内的轻微滞后。

如果要更强一致，可以继续升级：

- 列表 key 增加版本号，例如 `post_comment_version:{post_id}`。
- 写评论/删评论/审核时递增版本号，让旧列表 key 自然失效。
- 或者维护 `post_id -> list keys` 集合，写操作直接删除集合里的精确 key，避免 SCAN 前缀。

## 7. 风险和改进

| 风险 | 当前处理 | 后续优化 |
| --- | --- | --- |
| 缓存穿透 | Bloom + 空值缓存 | 启动时预热 Bloom，避免误判 |
| 缓存击穿 | singleflight | 热点 key 可加逻辑过期 |
| 缓存雪崩 | TTL ±10% 抖动 | 不同业务再分散 TTL |
| 大分页 | `page_size <= 100` | 压测 page_size=100，观察 MGET 和 DTO 组装 |
| 角色权限污染 | 查询条件仍在 DB 层控制；对象缓存复用 | 命中对象缓存后补角色可见性校验 |
| 列表短暂不一致 | 写操作主动删列表 ID 缓存，短 TTL 兜底 | 版本号 key 或 key 集合 |

## 8. 面试说法

> 评论列表缓存一开始可以直接缓存整页对象，但这样会造成大 value 和重复缓存。后面我把它拆成两层：列表 key 只存 comment_id 和 total，评论内容走 `mysql:comment:{comment_id}` 对象缓存。读列表时先拿 ID 列表，再 MGET 评论对象，miss 的部分批量查 MySQL 并回填。写路径不主动加载缓存，只删除受影响的对象缓存和列表 ID 缓存；新增、删除、审核时不手动改列表缓存，因为分页、排序和 total 很容易改错。下一次查询从 MySQL 重新构建缓存。TTL 只作为兜底，并加 ±10% 抖动避免雪崩。
