package data

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"comment-service/dal/model"
	"comment-service/dal/query"
	"comment-service/internal/biz"
	"comment-service/internal/cachecontrol"
	"comment-service/internal/observability"

	"github.com/go-kratos/kratos/v2/log"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

// 学生端缓存 key、TTL 和 Bloom Filter 相关常量。
//
// 这里把对象缓存、列表缓存、空值缓存分开命名，方便读写路径明确知道自己操作的是哪类缓存。
const (
	// 学生端复用 MySQL 对象缓存。
	//
	// 这些 key 和 tutor/operator 端可以共用同一份对象缓存：
	// mysql:post:{post_id}
	// mysql:comment:{comment_id}
	studentPostCachePrefix    = "mysql:post:"
	studentCommentCachePrefix = "mysql:comment:"
	postLikeStatusCachePrefix = "mysql:post_like:"

	// 学生端列表缓存。
	//
	// 列表缓存受新增、删除、点赞、评论影响较多，
	// 所以使用短 TTL，不做复杂精准删除。
	studentPostCommentListCachePrefix = "mysql:student:post_comments:"
	studentMyCommentListCachePrefix   = "mysql:student:my_comments:"

	// Bloom Filter key。
	//
	// Bloom 只用于按 ID 查询，拦截明显不存在的 post_id/comment_id。
	studentBloomPostKey    = "bf:post"
	studentBloomCommentKey = "bf:comment"

	// 缓存 TTL。
	studentObjectCacheTTL  = 10 * time.Minute
	studentListCacheTTL    = 2 * time.Minute
	studentNullCacheTTL    = 30 * time.Second
	postLikeStatusCacheTTL = 30 * time.Minute

	studentCommentListCacheVersion = 2

	studentNullCacheValue = "__nil__"

	// postLikeTxnMaxAttempts 是点赞关系写操作的最大重试次数。
	//
	// 计数已经异步化，但 post_like 唯一键/状态更新仍可能在热点并发下遇到短暂冲突。
	postLikeTxnMaxAttempts = 5

	postLikeStatusInactive int32 = 0
	postLikeStatusActive   int32 = 1
)

// errPostLikeLockBusy 复用 biz 层哨兵错误，便于 service 层统一映射成 429。
var errPostLikeLockBusy = biz.ErrPostLikeBusy

// studentRepo 实现学生端的数据访问能力。
//
// 该仓储负责学生侧点赞、评论、帖子详情等链路中的 MySQL、Redis、Bloom 和 singleflight 组合逻辑。
type studentRepo struct {
	data *Data
	log  *log.Helper

	// mysqlGroup 用于防止缓存击穿。
	//
	// 同一个 post_id/comment_id 或同一个列表查询缓存失效时，
	// 只允许一个 goroutine 查询 MySQL，其余请求复用结果。
	mysqlGroup singleflight.Group
}

// NewStudentRepo 创建学生端数据仓储。
func NewStudentRepo(data *Data, logger log.Logger) biz.StudentRepo {
	return &studentRepo{
		data: data,
		log:  log.NewHelper(log.With(logger, "module", "data/student")),
	}
}

// ============================================================
// 一、帖子点赞写操作
// ============================================================

// LikePost 点赞帖子。
//
// 写入内容：
// 1. 用单条 upsert 更新 post_like 点赞状态，减少热点并发下的锁范围；
// 2. 状态真实变化后把 like_count delta 写入 Redis 队列，由 comment-task 异步批量落到 post_counter。
//
// 缓存处理：
// 请求链路不删除帖子统计缓存；详情读取时把缓存基础值与 Redis pending delta 相加。
func (r *studentRepo) LikePost(ctx context.Context, studentID, postID int64) error {
	// 同一学生对同一帖子点赞属于热点幂等写，先用短 Redis 锁收敛并发。
	// 如果 Redis 本身异常，降级依赖数据库约束；如果只是锁竞争超时，则直接返回操作频繁。
	unlock, err := r.lockPostLike(ctx, studentID, postID)
	if errors.Is(err, errPostLikeLockBusy) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		r.log.WithContext(ctx).Warnf("lock post like failed, fallback to db constraint, student_id=%d, post_id=%d, err=%v", studentID, postID, err)
	} else {
		defer unlock()
	}

	// 点赞状态缓存必须在短锁内检查：等待锁的重复请求可以复用前一个请求刚写入的状态，
	// 命中“已点赞”后直接幂等返回，避免再次访问 MySQL 唯一索引。
	status, hit, cacheErr := r.getPostLikeStatusCache(ctx, postID, studentID)
	if cacheErr != nil {
		r.log.WithContext(ctx).Warnf("get post like status cache failed, fallback to db, student_id=%d, post_id=%d, err=%v", studentID, postID, cacheErr)
	} else if hit && status == postLikeStatusActive {
		return nil
	}

	changed, err := r.runPostLikeWriteWithRetry(ctx, "like", studentID, postID, func() (bool, error) {
		result := r.data.q.PostLike.WithContext(ctx).UnderlyingDB().Exec(`
INSERT INTO post_like (post_id, student_id, status)
VALUES (?, ?, 1)
ON DUPLICATE KEY UPDATE
	status = IF(status = 0, 1, status),
	updated_at = IF(status = 0, CURRENT_TIMESTAMP, updated_at)
`, postID, studentID)
		if result.Error != nil {
			return false, result.Error
		}

		// RowsAffected：新增为 1，历史取消恢复为 2，已经点赞的 no-op 为 0。
		return result.RowsAffected > 0, nil
	})
	if err != nil {
		return err
	}

	// MySQL 成功后再写状态缓存，不能让 Redis 中出现数据库尚未确认的点赞关系。
	if err := r.setPostLikeStatusCache(ctx, postID, studentID, postLikeStatusActive); err != nil {
		r.log.WithContext(ctx).Warnf("set post like status cache failed, student_id=%d, post_id=%d, err=%v", studentID, postID, err)
	}

	if changed {
		// 点赞计数不在请求内同步更新 post 表，只写 +1 delta 等待 comment-task 刷入 post_counter。
		if err := r.recordPostLikeCountDelta(ctx, postID, 1); err != nil {
			return err
		}
	}

	return nil
}

// UnlikePost 取消点赞。
//
// 写入内容：
// 1. 用单条 update 将 post_like.status 改为 0；
// 2. 如果确实取消成功，把 like_count=-1 delta 写入 Redis 队列，由 comment-task 异步批量落到 post_counter。
//
// 缓存处理：
// 请求链路不删除帖子统计缓存；comment-task 落库成功后统一失效统计缓存。
func (r *studentRepo) UnlikePost(ctx context.Context, studentID, postID int64) error {
	// 取消点赞和点赞使用同一把锁，避免一边点赞一边取消导致状态和计数交叉错乱。
	unlock, err := r.lockPostLike(ctx, studentID, postID)
	if errors.Is(err, errPostLikeLockBusy) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if err != nil {
		r.log.WithContext(ctx).Warnf("lock post unlike failed, fallback to db constraint, student_id=%d, post_id=%d, err=%v", studentID, postID, err)
	} else {
		defer unlock()
	}

	status, hit, cacheErr := r.getPostLikeStatusCache(ctx, postID, studentID)
	if cacheErr != nil {
		r.log.WithContext(ctx).Warnf("get post unlike status cache failed, fallback to db, student_id=%d, post_id=%d, err=%v", studentID, postID, cacheErr)
	} else if hit && status == postLikeStatusInactive {
		return biz.ErrPostNotLiked
	}

	changed, err := r.runPostLikeWriteWithRetry(ctx, "unlike", studentID, postID, func() (bool, error) {
		result := r.data.q.PostLike.WithContext(ctx).UnderlyingDB().Exec(`
UPDATE post_like
SET status = 0, updated_at = CURRENT_TIMESTAMP
WHERE post_id = ? AND student_id = ? AND status = 1
`, postID, studentID)
		if result.Error != nil {
			return false, result.Error
		}
		if result.RowsAffected == 0 {
			// 本来就没点赞或已经取消过，返回明确业务错误，避免重复取消冒泡成 5xx。
			return false, biz.ErrPostNotLiked
		}
		return true, nil
	})
	if err != nil {
		// 数据库确认当前没有有效点赞关系后，回填 status=0，后续重复取消无需再访问 MySQL。
		if errors.Is(err, biz.ErrPostNotLiked) {
			if cacheErr := r.setPostLikeStatusCache(ctx, postID, studentID, postLikeStatusInactive); cacheErr != nil {
				r.log.WithContext(ctx).Warnf("set post unlike status cache failed, student_id=%d, post_id=%d, err=%v", studentID, postID, cacheErr)
			}
		}
		return err
	}

	if err := r.setPostLikeStatusCache(ctx, postID, studentID, postLikeStatusInactive); err != nil {
		r.log.WithContext(ctx).Warnf("set post unlike status cache failed, student_id=%d, post_id=%d, err=%v", studentID, postID, err)
	}

	if changed {
		// 取消点赞同样不在请求内同步更新 post 表，只写 -1 delta 等待 comment-task 刷入 post_counter。
		// 这可以覆盖“大量用户同时取消同一帖子点赞”的热点写场景。
		if err := r.recordPostLikeCountDelta(ctx, postID, -1); err != nil {
			return err
		}
	}

	return nil
}

// runPostLikeWriteWithRetry 对点赞/取消点赞关系写做轻量重试。
//
// MySQL 在热点点赞关系写入时仍可能返回 1213 deadlock 或 1205 lock wait timeout；
// Redis 锁只收敛同一学生的重复请求，不会串行化不同学生对同一帖子的点赞。
// 这类错误通常重跑短写操作即可恢复，最终仍由唯一索引保证幂等与计数一致。
func (r *studentRepo) runPostLikeWriteWithRetry(
	ctx context.Context,
	action string,
	studentID, postID int64,
	fn func() (bool, error),
) (bool, error) {
	var lastErr error
	for attempt := 1; attempt <= postLikeTxnMaxAttempts; attempt++ {
		changed, err := fn()
		if err == nil {
			return changed, nil
		}

		lastErr = err
		retryable := isRetryablePostLikeWriteError(err)
		if !retryable {
			return false, err
		}
		if attempt == postLikeTxnMaxAttempts {
			r.log.WithContext(ctx).Warnf(
				"post like write still busy after retries, action=%s, student_id=%d, post_id=%d, attempts=%d, err=%v",
				action, studentID, postID, attempt, err,
			)
			return false, biz.ErrPostLikeBusy
		}

		r.log.WithContext(ctx).Warnf(
			"retry post like write, action=%s, student_id=%d, post_id=%d, attempt=%d, err=%v",
			action, studentID, postID, attempt, err,
		)

		if err := sleepPostLikeRetry(ctx, attempt); err != nil {
			return false, err
		}
	}

	return false, lastErr
}

// isRetryablePostLikeWriteError 判断 MySQL 热点写冲突是否适合重试。
func isRetryablePostLikeWriteError(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}

	switch mysqlErr.Number {
	case 1062, // Duplicate entry，Redis 降级时同一学生并发插入，重试后会走幂等已点赞分支。
		1205, // Lock wait timeout exceeded.
		1213: // Deadlock found when trying to get lock.
		return true
	default:
		return false
	}
}

func sleepPostLikeRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt*20) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *studentRepo) recordPostLikeCountDelta(ctx context.Context, postID, delta int64) error {
	// delta 入 Redis 成功后，请求即可返回；真正更新 post_counter 的动作在 comment-task 中批量执行。
	if err := r.data.enqueuePostLikeCountDelta(ctx, postID, delta); err != nil {
		r.log.WithContext(ctx).Warnf("enqueue post like count delta failed, post_id=%d, delta=%d, err=%v", postID, delta, err)
		// post_like 是点赞关系事实表；Redis/Kafka 计数属于异步冗余链路。
		// 远程 Redis 短暂抖动时不让用户请求失败，后续可通过 post_like 事实表做计数校准。
		return nil
	}

	// 点赞请求不删除帖子对象缓存。缓存中的 like_count 作为 post_counter 已落库基础值，
	// 详情读路径会动态叠加 Redis pending delta，因此无需让每次点赞都触发缓存回源。
	// comment-task 将 delta 成功写入 MySQL 后会删除该缓存，下一次读取再加载新的基础值。
	return nil
}

// ============================================================
// 二、评论写操作
// ============================================================

// CreateComment 发表评论。
//
// 事务内容：
// 1. 校验知识帖存在；
// 2. 插入 study_comment。
// 评论数变化在事务提交后写入 Redis delta，由 comment-task 聚合落库。
//
// 缓存处理：
// 1. 评论创建成功后，将 comment_id 加入 Bloom Filter；
// 2. 删除帖子评论、我的评论、运营审核等列表 ID 缓存，下次查询回源重建。
//
// 注意：写路径不主动写入 mysql:comment:{comment_id}。
// 评论对象缓存只在读路径命中 MySQL 后回填，避免写路径把未被读取的数据预热进缓存。
func (r *studentRepo) CreateComment(ctx context.Context, comment *model.StudyComment) (*model.StudyComment, error) {
	err := r.data.q.Transaction(func(tx *query.Query) error {
		c := tx.StudyComment
		p := tx.Post

		if _, err := p.WithContext(ctx).
			Where(p.PostID.Eq(comment.PostID), p.Status.Eq(1), p.DeletedAt.IsNull()).
			First(); err != nil {
			return errors.New("知识帖不存在")
		}
		if err := c.WithContext(ctx).Create(comment); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	_ = r.bloomAdd(ctx, studentBloomCommentKey, comment.CommentID)

	r.data.recordPostCommentCountDelta(ctx, comment.PostID, 1)
	if err := deleteCommentListCaches(ctx, r.data.cache, comment.PostID, comment.StudentID, comment.AuditStatus); err != nil {
		r.log.WithContext(ctx).Warnf("delete comment list caches after create comment failed, post_id=%d, student_id=%d, err=%v", comment.PostID, comment.StudentID, err)
	}

	return comment, nil
}

// DeleteComment 学生删除自己的评论。
//
// 事务内容：
// 1. 查询评论所属 post_id；
// 2. visible_status 置为 2；
// 3. 设置 deleted_at；
// 4. 事务提交后把 comment_count=-1 写入 Redis delta，后续异步落到 post_counter。
//
// 缓存处理：
// 1. 删除 mysql:comment:{comment_id}；
// 2. 删除相关列表 ID 缓存，下次查询回源重建。
//
// 注意：删除写路径也不主动写入 __nil__ 空值缓存。
// 空值缓存只在读路径确认 DB 不存在后再写入。
func (r *studentRepo) DeleteComment(ctx context.Context, commentID int64) error {
	var postID int64
	var studentID int64
	var auditStatus int32
	var needDecrement bool

	err := r.data.q.Transaction(func(tx *query.Query) error {
		c := tx.StudyComment

		// 先读取原始 visible_status，后面只有“原本可见且本次确实删除成功”才扣评论数。
		comment, err := c.WithContext(ctx).
			Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
			First()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		postID = comment.PostID
		studentID = comment.StudentID
		auditStatus = comment.AuditStatus
		needDecrement = comment.VisibleStatus == 1

		// 条件更新用 deleted_at IS NULL 做并发兜底，重复删除不会重复扣计数。
		info, err := c.WithContext(ctx).
			Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
			Updates(map[string]interface{}{
				"visible_status": 2,
				"deleted_at":     time.Now(),
			})
		if err != nil {
			return err
		}
		if info.RowsAffected == 0 {
			needDecrement = false
			return nil
		}

		return nil
	})
	if err != nil {
		return err
	}
	if needDecrement {
		r.data.recordPostCommentCountDelta(ctx, postID, -1)
	}

	commentKey := buildStudentCommentCacheKey(commentID)
	_ = r.delCache(ctx, commentKey)

	if err := deleteCommentListCaches(ctx, r.data.cache, postID, studentID, auditStatus); err != nil {
		r.log.WithContext(ctx).Warnf("delete comment list caches after delete comment failed, comment_id=%d, post_id=%d, student_id=%d, err=%v", commentID, postID, studentID, err)
	}

	return nil
}

// ============================================================
// 三、帖子读操作
// ============================================================

// GetPostByID 根据 post_id 查询已发布帖子。
//
// 查询链路：
// Bloom Filter
//
//	↓
//
// Redis 对象缓存
//
//	↓
//
// singleflight
//
//	↓
//
// MySQL
//
//	↓
//
// 写 Redis + 写 Bloom Filter
func (r *studentRepo) GetPostByID(ctx context.Context, postID int64) (post *model.Post, err error) {
	ctx, span := observability.StartSpan(ctx, "studentRepo.GetPostByID",
		attribute.Int64("post_id", postID),
	)
	defer func() { observability.EndSpan(span, err) }()

	if postID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// Bloom Filter 是按 ID 查询的第一道防穿透保护。
	// 如果明确不存在，可以直接返回；RedisBloom 异常时只记录日志并继续主流程。
	exists, err := r.bloomExists(ctx, studentBloomPostKey, postID)
	if err != nil {
		r.log.WithContext(ctx).Warnf("bloom post exists failed, post_id=%d, err=%v", postID, err)
	}
	if err == nil && !exists {
		return nil, gorm.ErrRecordNotFound
	}

	post, err = loadPostWithSplitCache(
		ctx,
		r.data.cache,
		&r.mysqlGroup,
		postID,
		studentNullCacheValue,
		studentObjectCacheTTL,
		studentObjectCacheTTL,
		studentNullCacheTTL,
		r.queryPostByIDFromDB,
		r.queryPostStatsByIDFromDB,
	)
	if err != nil {
		return nil, err
	}
	// 主体/统计缓存由三端共享；学生端命中缓存后仍要执行角色可见性校验。
	if post.Status != 1 || post.DeletedAt.Valid {
		return nil, gorm.ErrRecordNotFound
	}

	_ = r.bloomAdd(ctx, studentBloomPostKey, postID)
	r.data.applyPendingPostLikeCountDelta(ctx, post)
	r.data.applyPendingPostCommentCountDelta(ctx, post)
	return post, nil
}

// ============================================================
// 四、评论读操作
// ============================================================

// GetCommentByID 根据 comment_id 查询评论。
//
// 查询链路：
// Bloom Filter
//
//	↓
//
// Redis 对象缓存
//
//	↓
//
// singleflight
//
//	↓
//
// MySQL
func (r *studentRepo) GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	if commentID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// Bloom Filter 快速判断 comment_id 是否可能存在，减少明显无效 ID 的存储访问。
	exists, err := r.bloomExists(ctx, studentBloomCommentKey, commentID)
	if err != nil {
		r.log.WithContext(ctx).Warnf("bloom comment exists failed, comment_id=%d, err=%v", commentID, err)
	}
	if err == nil && !exists {
		return nil, gorm.ErrRecordNotFound
	}

	key := buildStudentCommentCacheKey(commentID)

	// 读取评论对象缓存。这里不限制 visible_status，调用方根据场景决定是否允许展示。
	comment, hit, err := r.getCommentFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get comment cache failed, key=%s, err=%v", key, err)
	} else if hit {
		if comment == nil {
			return nil, gorm.ErrRecordNotFound
		}
		return comment, nil
	}

	// 缓存未命中后合并同一 comment_id 的 MySQL 查询，避免详情/删除校验并发击穿。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 用于复用其他请求刚刚写入的缓存。
		comment, hit, err := r.getCommentFromCache(ctx, key)
		if err == nil && hit {
			return comment, nil
		}

		// 回源 MySQL。不存在写空值缓存，存在写对象缓存并补 Bloom。
		comment, err = r.queryCommentByIDFromDB(ctx, commentID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = r.setCache(ctx, key, []byte(studentNullCacheValue), studentNullCacheTTL)
			}
			return nil, err
		}

		if data, err := json.Marshal(comment); err == nil {
			_ = r.setCache(ctx, key, data, studentObjectCacheTTL)
		}

		_ = r.bloomAdd(ctx, studentBloomCommentKey, commentID)

		return comment, nil
	})
	if err != nil {
		return nil, err
	}

	comment, ok := val.(*model.StudyComment)
	if !ok || comment == nil {
		return nil, gorm.ErrRecordNotFound
	}

	return comment, nil
}

// ListPostCommentsStudent 查询某个帖子下学生端可见评论。
//
// 查询链路：
// Redis 列表 ID 缓存
//
//	↓
//
// Redis MGET 评论对象缓存
//
//	↓
//
// singleflight + MySQL 分页查询或按 ID 批量补 miss
//
// 注意：
// 1. 列表缓存只保存 comment_id 和 total，不保存整页评论对象；
// 2. 评论对象统一走 mysql:comment:{comment_id}，详情页和列表页可以复用；
// 3. 评论新增/删除会主动删除相关列表 ID 缓存，短 TTL 只做异常兜底。
func (r *studentRepo) ListPostCommentsStudent(ctx context.Context, postID int64, pageNum, pageSize int32) (comments []*model.StudyComment, total int64, err error) {
	// 顶层 span 覆盖整个列表查询，用于在链路追踪中观察本次请求的分页参数、
	// 缓存命中情况、返回评论数和 total。这里使用具名返回值，defer 中可以统一记录最终错误。
	ctx, span := observability.StartSpan(ctx, "studentRepo.ListPostCommentsStudent",
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	defer func() { observability.EndSpan(span, err) }()

	// 统一修正分页参数，避免 pageNum/pageSize 异常导致缓存 key 分裂或 MySQL 查询过大。
	// 修正后的参数会同时用于 Redis 列表缓存和 MySQL 分页查询，保证两边语义一致。
	pageNum, pageSize = normalizeStudentPage(pageNum, pageSize)

	// 列表缓存 key 只描述“某个帖子 + 某一页”的评论 ID 列表。
	// 缓存值不保存完整评论对象，评论详情会通过对象缓存 mysql:comment:{comment_id} 复用。
	key := buildStudentPostCommentListCacheKey(postID, pageNum, pageSize)

	// 第一层 Redis 读取：优先拿帖子评论列表的 ID 缓存。
	// 如果命中，后续只需要按 ID 批量加载评论对象，不需要再做分页查询和 COUNT。
	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.GET student:post_comments",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "GET"),
		attribute.String("cache.key", key),
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	cache, hit, err := r.getCommentListFromCache(cacheCtx, key)
	cacheSpan.SetAttributes(attribute.Bool("cache.hit", hit))
	observability.EndSpan(cacheSpan, err)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get student post comments cache failed, key=%s, err=%v", key, err)
	} else if hit {
		// 列表 ID 命中后，再批量加载评论对象。
		// queryVisibleCommentsByIDsFromDB 会兜底过滤学生端可见评论，避免对象缓存缺失时返回不可见数据。
		comments, err = r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
		if err != nil {
			return nil, 0, err
		}
		span.SetAttributes(
			attribute.Bool("cache.hit", true),
			attribute.Int("comment_ids.count", len(cache.CommentIDs)),
			attribute.Int("comments.count", len(comments)),
			attribute.Int64("total_comments", cache.Total),
		)
		return comments, total, nil
	}

	// 缓存未命中时进入 singleflight，防止同一个分页 key 在高并发下同时打到 MySQL。
	// 只有一个 goroutine 负责回源和重建缓存，其余相同请求复用它返回的列表缓存结构。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// singleflight 内部再做一次 Redis double check。
		// 这样可以覆盖“等待锁期间其他请求已经把缓存建好”的情况，减少不必要的 MySQL 查询。
		cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.GET student:post_comments.double_check",
			attribute.String("db.system", "redis"),
			attribute.String("db.operation", "GET"),
			attribute.String("cache.key", key),
			attribute.Int64("post_id", postID),
			attribute.Int("page_num", int(pageNum)),
			attribute.Int("page_size", int(pageSize)),
		)
		cache, hit, err := r.getCommentListFromCache(cacheCtx, key)
		cacheSpan.SetAttributes(attribute.Bool("cache.hit", hit))
		observability.EndSpan(cacheSpan, err)
		if err == nil && hit {
			return cache, nil
		}

		// double check 仍未命中时才真正回源 MySQL。
		// 这里会查询当前页学生端可见评论，并同时 COUNT 得到 total，作为接口分页总数返回。
		comments, total, err := r.queryPostCommentsStudentFromDB(ctx, postID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// MySQL 查出的完整评论对象先回填对象缓存。
		// 列表缓存只保存 ID 和 total，后续详情页、其他列表页都能复用对象缓存。
		r.setCommentObjectCaches(ctx, comments)

		// 用本次 MySQL 结果重建列表 ID 缓存。
		// 短 TTL 用来降低缓存失效维护成本，评论新增/删除也会主动删除相关列表缓存。
		cache = newStudentCommentListCache(comments, total)

		if data, err := json.Marshal(cache); err == nil {
			_ = r.setCache(ctx, key, data, studentListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// singleflight 返回的是统一的列表缓存结构。
	// 这里做类型校验，避免未来改造返回值时出现静默错误。
	cache, ok := val.(*studentCommentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid student post comment list cache result")
	}

	// 无论 singleflight 内部是 double check 命中还是 MySQL 回源，
	// 最终都按 comment_ids 再加载一次评论对象，确保返回顺序和列表缓存中的 ID 顺序一致。
	comments, err = r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
	if err != nil {
		return nil, 0, err
	}

	// 记录最终返回规模。cache.hit=false 表示第一次读取列表缓存未命中，
	// 但 singleflight 内部仍可能通过 double check 复用了其他请求刚写入的缓存。
	span.SetAttributes(
		attribute.Bool("cache.hit", false),
		attribute.Int("comment_ids.count", len(cache.CommentIDs)),
		attribute.Int("comments.count", len(comments)),
		attribute.Int64("total_comments", cache.Total),
	)
	// 返回当前页学生可见评论列表，以及满足条件的评论总数 total。
	return comments, total, nil
}

// ListMyComments 查询学生自己的评论列表。
//
// 查询链路：
// Redis 列表 ID 缓存
//
//	↓
//
// Redis MGET 评论对象缓存
//
//	↓
//
// singleflight + MySQL 分页查询或按 ID 批量补 miss
func (r *studentRepo) ListMyComments(ctx context.Context, studentID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	// 统一分页，保证缓存 key 与 MySQL offset 使用同一套合法参数。
	pageNum, pageSize = normalizeStudentPage(pageNum, pageSize)

	// 我的评论列表按 student_id + page 参数生成缓存 key。
	// value 仍然只保存 comment_id 列表和 total，不保存完整评论对象。
	key := buildStudentMyCommentListCacheKey(studentID, pageNum, pageSize)

	// 先读列表 ID 缓存。命中后再通过对象缓存/Mysql miss 回源加载评论对象。
	cache, hit, err := r.getCommentListFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get my comments cache failed, key=%s, err=%v", key, err)
	} else if hit {
		comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
		return comments, cache.Total, err
	}

	// 列表未命中时使用 singleflight 合并同一学生同一页的 MySQL 回源。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 处理等待期间其他请求已经重建缓存的情况。
		cache, hit, err := r.getCommentListFromCache(ctx, key)
		if err == nil && hit {
			return cache, nil
		}

		// 回源查询该学生自己的评论，不额外限制 visible_status，便于展示审核/可见状态。
		comments, total, err := r.queryMyCommentsFromDB(ctx, studentID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// 回填评论对象缓存，列表缓存保持轻量 ID 结构。
		r.setCommentObjectCaches(ctx, comments)

		// 重建我的评论列表缓存。新增/删除评论会按 student_id 前缀清理它。
		cache = newStudentCommentListCache(comments, total)

		if data, err := json.Marshal(cache); err == nil {
			_ = r.setCache(ctx, key, data, studentListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// singleflight 返回列表缓存结构后，再按 ID 顺序加载评论对象，保证返回顺序稳定。
	cache, ok := val.(*studentCommentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid my comments cache result")
	}

	comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
	return comments, cache.Total, err
}

// ============================================================
// 六、聚合查询
// ============================================================

// GetPostDetailWithComments 查询帖子详情及评论列表。
//
// 复用已有缓存方法：
// 1. GetPostByID 负责帖子对象缓存；
// 2. 帖子详情页复用 post_counter.comment_count 作为总数，避免额外 COUNT(*);
// 3. 评论列表走 Redis 列表 ID 缓存，评论对象再通过 MGET 批量读取。
func (r *studentRepo) GetPostDetailWithComments(
	ctx context.Context,
	postID int64,
	pageNum, pageSize int32,
) (post *model.Post, comments []*model.StudyComment, total int64, err error) {
	ctx, span := observability.StartSpan(ctx, "studentRepo.GetPostDetailWithComments",
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	defer func() {
		span.SetAttributes(
			attribute.Int("comments.count", len(comments)),
			attribute.Int64("total_comments", total),
		)
		observability.EndSpan(span, err)
	}()

	post, err = r.GetPostByID(ctx, postID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, 0, errors.New("帖子不存在或已下架")
		}
		return nil, nil, 0, err
	}

	comments, total, err = r.listPostCommentsStudentForDetail(ctx, post, pageNum, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}

	return post, comments, total, nil
}

// listPostCommentsStudentForDetail 查询详情页需要的当前页评论。
//
// 详情页的总评论数直接使用 post_counter.comment_count：
// 1. post_counter 已落库 comment_count 与 Redis pending delta 已在帖子读取时合并；
// 2. 详情页不需要为了展示总数再对 study_comment 做 COUNT(*);
// 3. 当 comment_count=0 时可以直接返回空列表，完全跳过 MySQL 评论查询。
func (r *studentRepo) listPostCommentsStudentForDetail(ctx context.Context, post *model.Post, pageNum, pageSize int32) (comments []*model.StudyComment, total int64, err error) {
	if post == nil {
		return nil, 0, gorm.ErrRecordNotFound
	}

	pageNum, pageSize = normalizeStudentPage(pageNum, pageSize)
	total = int64(post.CommentCount)
	if total <= 0 {
		return []*model.StudyComment{}, 0, nil
	}

	key := buildStudentPostCommentListCacheKey(post.PostID, pageNum, pageSize)

	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.GET student:post_comments.detail",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "GET"),
		attribute.String("cache.key", key),
		attribute.Int64("post_id", post.PostID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	cache, hit, err := r.getCommentListFromCache(cacheCtx, key)
	cacheSpan.SetAttributes(attribute.Bool("cache.hit", hit))
	observability.EndSpan(cacheSpan, err)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get student detail comments cache failed, key=%s, err=%v", key, err)
	} else if hit {
		comments, err = r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
		if err != nil {
			return nil, 0, err
		}
		return comments, cache.Total, nil
	}

	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.GET student:post_comments.detail.double_check",
			attribute.String("db.system", "redis"),
			attribute.String("db.operation", "GET"),
			attribute.String("cache.key", key),
			attribute.Int64("post_id", post.PostID),
			attribute.Int("page_num", int(pageNum)),
			attribute.Int("page_size", int(pageSize)),
		)
		cache, hit, err := r.getCommentListFromCache(cacheCtx, key)
		cacheSpan.SetAttributes(attribute.Bool("cache.hit", hit))
		observability.EndSpan(cacheSpan, err)
		if err == nil && hit {
			return cache, nil
		}

		comments, err := r.queryPostCommentItemsStudentFromDB(ctx, post.PostID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		r.setCommentObjectCaches(ctx, comments)

		cache = newStudentCommentListCache(comments, total)
		if data, err := json.Marshal(cache); err == nil {
			_ = r.setCache(ctx, key, data, studentListCacheTTL)
		}
		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	cache, ok := val.(*studentCommentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid student detail comment list cache result")
	}
	comments, err = r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
	if err != nil {
		return nil, 0, err
	}
	return comments, cache.Total, nil
}

// ============================================================
// 七、MySQL 原始查询方法
//
// 说明：
// 1. 这些方法只查 DB；
// 2. 不读写缓存；
// 3. 外层方法负责 Redis、Bloom、singleflight。
// ============================================================

// queryPostByIDFromDB 从 MySQL 查询学生端可见帖子。
//
// 返回 status=1 且未删除的帖子；不读写 Redis。
func (r *studentRepo) queryPostByIDFromDB(ctx context.Context, postID int64) (post *model.Post, err error) {
	ctx, span := observability.StartSpan(ctx, "mysql.SELECT post",
		attribute.String("db.system", "mysql"),
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "post"),
		attribute.Int64("post_id", postID),
	)
	defer func() { observability.EndSpan(span, err) }()

	p := r.data.q.Post

	post, err = p.WithContext(ctx).
		Where(p.PostID.Eq(postID), p.Status.Eq(1), p.DeletedAt.IsNull()).
		First()
	if err != nil {
		return nil, err
	}
	if err := r.data.attachPersistedPostCounters(ctx, []*model.Post{post}); err != nil {
		return nil, err
	}
	return post, nil
}

// queryPostStatsByIDFromDB 只读取帖子统计字段。
// 当主体缓存命中而统计缓存失效时，只回源 post_counter，不访问 post 主表的高频字段。
func (r *studentRepo) queryPostStatsByIDFromDB(ctx context.Context, postID int64) (*postStatsCache, error) {
	stats, err := r.data.queryPostCounterByID(ctx, postID)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// queryCommentByIDFromDB 从 MySQL 按 comment_id 查询评论。
//
// 返回未删除评论；是否可见由上层业务决定。
func (r *studentRepo) queryCommentByIDFromDB(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
		First()
}

// queryVisibleCommentsByIDsFromDB 批量查询学生端可见评论。
//
// 返回 visible_status=1 且未删除的评论，用于帖子评论列表和详情页补缓存 miss。
func (r *studentRepo) queryVisibleCommentsByIDsFromDB(ctx context.Context, commentIDs []int64) (comments []*model.StudyComment, err error) {
	ctx, span := observability.StartSpan(ctx, "mysql.SELECT study_comment.batch_by_ids",
		attribute.String("db.system", "mysql"),
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "study_comment"),
		attribute.String("visibility", "student_visible"),
		attribute.Int("comment_ids.count", len(commentIDs)),
	)
	defer func() {
		span.SetAttributes(attribute.Int("comments.count", len(comments)))
		observability.EndSpan(span, err)
	}()

	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.In(commentIDs...), c.VisibleStatus.Eq(1), c.DeletedAt.IsNull()).
		Find()
}

// queryCommentsByIDsFromDB 批量查询评论对象。
//
// 返回未删除评论，不额外限制 visible_status，主要用于“我的评论”列表补缓存 miss。
func (r *studentRepo) queryCommentsByIDsFromDB(ctx context.Context, commentIDs []int64) (comments []*model.StudyComment, err error) {
	ctx, span := observability.StartSpan(ctx, "mysql.SELECT study_comment.batch_by_ids",
		attribute.String("db.system", "mysql"),
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "study_comment"),
		attribute.String("visibility", "student_own"),
		attribute.Int("comment_ids.count", len(commentIDs)),
	)
	defer func() {
		span.SetAttributes(attribute.Int("comments.count", len(comments)))
		observability.EndSpan(span, err)
	}()

	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.In(commentIDs...), c.DeletedAt.IsNull()).
		Find()
}

// queryPostCommentsStudentFromDB 查询帖子评论分页并同时 COUNT total。
//
// 返回当前页可见评论和总数，主要用于普通评论列表接口。
func (r *studentRepo) queryPostCommentsStudentFromDB(ctx context.Context, postID int64, pageNum, pageSize int32) (comments []*model.StudyComment, total int64, err error) {
	ctx, span := observability.StartSpan(ctx, "mysql.SELECT study_comment.list",
		attribute.String("db.system", "mysql"),
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "study_comment"),
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	defer func() {
		span.SetAttributes(
			attribute.Int("comments.count", len(comments)),
			attribute.Int64("total_comments", total),
		)
		observability.EndSpan(span, err)
	}()

	c := r.data.q.StudyComment

	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.PostID.Eq(postID), c.VisibleStatus.Eq(1), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
}

// queryPostCommentItemsStudentFromDB 只查询帖子评论当前页 items。
//
// 返回当前页评论，不再 COUNT；详情页 total 使用 post_counter.comment_count，避免额外慢 COUNT。
func (r *studentRepo) queryPostCommentItemsStudentFromDB(ctx context.Context, postID int64, pageNum, pageSize int32) (comments []*model.StudyComment, err error) {
	ctx, span := observability.StartSpan(ctx, "mysql.SELECT study_comment.items",
		attribute.String("db.system", "mysql"),
		attribute.String("db.operation", "SELECT"),
		attribute.String("db.table", "study_comment"),
		attribute.String("total.source", "post_counter.comment_count"),
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	defer func() {
		span.SetAttributes(attribute.Int("comments.count", len(comments)))
		observability.EndSpan(span, err)
	}()

	c := r.data.q.StudyComment

	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.PostID.Eq(postID), c.VisibleStatus.Eq(1), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		Offset(offset).
		Limit(int(pageSize)).
		Find()
}

// queryMyCommentsFromDB 查询学生自己的评论分页。
//
// 返回当前页评论和 total；不限制 visible_status，学生可以在“我的评论”看到自己评论状态。
func (r *studentRepo) queryMyCommentsFromDB(ctx context.Context, studentID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	c := r.data.q.StudyComment

	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.StudentID.Eq(studentID), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
}

// ============================================================
// 八、缓存结构体
// ============================================================

type studentCommentListCache struct {
	Version    int     `json:"version"`
	CommentIDs []int64 `json:"comment_ids"`
	Total      int64   `json:"total"`
}

// newStudentCommentListCache 根据当前页评论构造列表 ID 缓存值。
//
// 返回值只保存 comment_id 顺序和 total，不保存整页评论对象。
func newStudentCommentListCache(comments []*model.StudyComment, total int64) *studentCommentListCache {
	return &studentCommentListCache{
		Version:    studentCommentListCacheVersion,
		CommentIDs: collectStudentCommentIDs(comments),
		Total:      total,
	}
}

// collectStudentCommentIDs 从评论模型列表中提取 comment_id。
//
// 返回值会跳过 nil 和非法 ID，保持原评论列表顺序。
func collectStudentCommentIDs(comments []*model.StudyComment) []int64 {
	ids := make([]int64, 0, len(comments))
	for _, comment := range comments {
		if comment == nil || comment.CommentID <= 0 {
			continue
		}
		ids = append(ids, comment.CommentID)
	}
	return ids
}

// ============================================================
// 九、对象缓存读取方法
// ============================================================

// getPostFromCache 从 Redis 读取帖子对象缓存。
//
// 返回值含义：
// 1. *model.Post：命中的帖子对象；
// 2. bool：是否命中缓存，包括命中 __nil__ 空值；
// 3. error：Redis 或 JSON 解析错误。
func (r *studentRepo) getPostFromCache(ctx context.Context, key string) (*model.Post, bool, error) {
	// getCache 返回原始 bytes；空结果表示未命中或绕过缓存。
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == studentNullCacheValue {
		// 命中空值缓存时返回 nil,true，调用方会把它当成“确定不存在”。
		return nil, true, nil
	}

	var post model.Post
	if err := json.Unmarshal(data, &post); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &post, true, nil
}

// getCommentFromCache 从 Redis 读取评论对象缓存。
//
// 返回值含义和 getPostFromCache 一致：comment、是否命中、错误。
func (r *studentRepo) getCommentFromCache(ctx context.Context, key string) (*model.StudyComment, bool, error) {
	// getCache 统一处理 Redis nil、cache bypass 和 Redis 不存在场景。
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == studentNullCacheValue {
		return nil, true, nil
	}

	var comment model.StudyComment
	if err := json.Unmarshal(data, &comment); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &comment, true, nil
}

// loadCommentsByIDs 按 comment_id 顺序加载评论对象。
//
// 流程：
// 1. 先 MGET 对象缓存；
// 2. miss 的 ID 批量查询 MySQL；
// 3. 回填对象缓存；
// 4. 按输入 commentIDs 的顺序返回评论列表。
func (r *studentRepo) loadCommentsByIDs(ctx context.Context, commentIDs []int64, queryMissed func(context.Context, []int64) ([]*model.StudyComment, error)) ([]*model.StudyComment, error) {
	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	// cached 是命中的评论 map；missed 是需要回源 MySQL 的 comment_id。
	cached, missed, err := r.getCommentsFromObjectCache(ctx, commentIDs)
	if err != nil {
		r.log.WithContext(ctx).Warnf("mget comment object cache failed, err=%v", err)
		missed = commentIDs
	}

	if len(missed) > 0 {
		// queryMissed 由调用方传入，用于区分“学生可见评论”和“我的评论”等不同查询条件。
		comments, err := queryMissed(ctx, missed)
		if err != nil {
			return nil, err
		}
		// MySQL 查到的数据回填对象缓存，下次列表/详情可以复用。
		r.setCommentObjectCaches(ctx, comments)
		for _, comment := range comments {
			if comment == nil {
				continue
			}
			cached[comment.CommentID] = comment
		}
	}

	result := make([]*model.StudyComment, 0, len(commentIDs))
	for _, commentID := range commentIDs {
		if comment := cached[commentID]; comment != nil {
			result = append(result, comment)
		}
	}

	return result, nil
}

// getCommentsFromObjectCache 批量读取评论对象缓存。
//
// 返回：
// 1. map[comment_id]*StudyComment：缓存命中的评论；
// 2. []int64：未命中的 comment_id；
// 3. error：Redis MGET 失败。
func (r *studentRepo) getCommentsFromObjectCache(ctx context.Context, commentIDs []int64) (map[int64]*model.StudyComment, []int64, error) {
	result := make(map[int64]*model.StudyComment, len(commentIDs))
	missed := make([]int64, 0, len(commentIDs))
	if len(commentIDs) == 0 {
		return result, missed, nil
	}
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		// 没有 Redis 或请求要求绕过缓存时，所有 ID 都视为 miss。
		return result, commentIDs, nil
	}

	keys := make([]string, 0, len(commentIDs))
	for _, commentID := range commentIDs {
		keys = append(keys, buildStudentCommentCacheKey(commentID))
	}

	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.MGET mysql:comment",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "MGET"),
		attribute.String("app.role", "student"),
		attribute.String("cache.key.prefix", studentCommentCachePrefix),
		attribute.Int("cache.keys.count", len(keys)),
	)
	// 一次 MGET 批量读取，避免每条评论一个 Redis 往返。
	values, err := r.data.cache.MGet(cacheCtx, keys...).Result()
	if err != nil {
		observability.EndSpan(cacheSpan, err)
		return result, commentIDs, err
	}

	for i, value := range values {
		commentID := commentIDs[i]
		if value == nil {
			missed = append(missed, commentID)
			continue
		}

		// redisValueBytes 兼容 Redis 返回 string 或 []byte 的情况。
		raw, ok := redisValueBytes(value)
		if !ok {
			missed = append(missed, commentID)
			continue
		}
		if string(raw) == studentNullCacheValue {
			missed = append(missed, commentID)
			continue
		}

		var comment model.StudyComment
		if err := json.Unmarshal(raw, &comment); err != nil {
			_ = r.delCache(ctx, keys[i])
			missed = append(missed, commentID)
			continue
		}
		if comment.CommentID <= 0 {
			_ = r.delCache(ctx, keys[i])
			missed = append(missed, commentID)
			continue
		}

		result[comment.CommentID] = &comment
	}

	cacheSpan.SetAttributes(
		attribute.Int("cache.hit.count", len(result)),
		attribute.Int("cache.miss.count", len(missed)),
	)
	observability.EndSpan(cacheSpan, nil)
	return result, missed, nil
}

// setCommentObjectCaches 将 MySQL 读到的评论回填到对象缓存。
//
// 只在读路径调用；写路径不会主动加载对象缓存。
func (r *studentRepo) setCommentObjectCaches(ctx context.Context, comments []*model.StudyComment) {
	for _, comment := range comments {
		if comment == nil || comment.CommentID <= 0 {
			continue
		}
		if data, err := json.Marshal(comment); err == nil {
			_ = r.setCache(ctx, buildStudentCommentCacheKey(comment.CommentID), data, studentObjectCacheTTL)
		}
		_ = r.bloomAdd(ctx, studentBloomCommentKey, comment.CommentID)
	}
}

// ============================================================
// 十、列表缓存读取方法
// ============================================================

// getCommentListFromCache 读取评论列表 ID 缓存。
//
// 返回值包含版本号、comment_ids 和 total；如果发现旧版本或 JSON 损坏，会删除缓存并返回未命中。
func (r *studentRepo) getCommentListFromCache(ctx context.Context, key string) (*studentCommentListCache, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var cache studentCommentListCache
	if err := json.Unmarshal(data, &cache); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}
	if cache.Version != studentCommentListCacheVersion {
		// 旧版本列表缓存保存的是整页评论对象。改成 ID 列表缓存后，
		// 旧值不能继续使用，直接删除并让本次请求回源重建。
		_ = r.delCache(ctx, key)
		return nil, false, nil
	}
	if cache.CommentIDs == nil {
		cache.CommentIDs = []int64{}
	}

	return &cache, true, nil
}

// ============================================================
// 十一、Redis 基础操作
// ============================================================

// getCache 读取 Redis string value。
//
// 返回 nil,nil 表示未命中、Redis 未配置或本次请求绕过缓存。
func (r *studentRepo) getCache(ctx context.Context, key string) ([]byte, error) {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil, nil
	}

	data, err := r.data.cache.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return data, nil
}

// setCache 写入 Redis 缓存，并统一加入 TTL 抖动。
//
// 返回 Redis SET 错误；如果 Redis 未配置或请求绕过缓存，直接返回 nil。
func (r *studentRepo) setCache(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Set(ctx, key, data, cacheTTLWithJitter(ttl)).Err()
}

// delCache 删除一个或多个 Redis key。
//
// 返回 Redis DEL 错误；没有 Redis 或 key 为空时直接返回 nil。
func (r *studentRepo) delCache(ctx context.Context, keys ...string) error {
	if r.data.cache == nil || len(keys) == 0 {
		return nil
	}

	return r.data.cache.Del(ctx, keys...).Err()
}

// getPostLikeStatusCache 读取某个学生对某个帖子的关系状态。
// hit=false 表示需要回退到 MySQL；缓存只用于快速幂等判断，不是点赞关系事实源。
func (r *studentRepo) getPostLikeStatusCache(ctx context.Context, postID, studentID int64) (status int32, hit bool, err error) {
	key := buildPostLikeStatusCacheKey(postID, studentID)
	data, err := r.getCache(ctx, key)
	if err != nil {
		return 0, false, err
	}
	if data == nil {
		return 0, false, nil
	}

	switch string(data) {
	case "1":
		return postLikeStatusActive, true, nil
	case "0":
		return postLikeStatusInactive, true, nil
	default:
		// 非法缓存值不参与业务判断，删除后按 miss 回退数据库。
		_ = r.delCache(ctx, key)
		return 0, false, nil
	}
}

// setPostLikeStatusCache 只在 MySQL 已经确认关系状态后调用。
func (r *studentRepo) setPostLikeStatusCache(ctx context.Context, postID, studentID int64, status int32) error {
	return r.setCache(
		ctx,
		buildPostLikeStatusCacheKey(postID, studentID),
		[]byte(strconv.FormatInt(int64(status), 10)),
		postLikeStatusCacheTTL,
	)
}

// lockPostLike 给同一学生同一帖子点赞/取消点赞加短锁。
//
// 返回 unlock 函数和错误。拿到锁后调用方必须 defer unlock()。
func (r *studentRepo) lockPostLike(ctx context.Context, studentID, postID int64) (func(), error) {
	if r.data.cache == nil {
		return func() {}, nil
	}

	// token 用于安全释放锁，避免误删其他请求后来拿到的新锁。
	key := fmt.Sprintf("lock:post_like:%d:%d", postID, studentID)
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	deadline := time.Now().Add(200 * time.Millisecond)

	for {
		// SetNX 返回 true 表示抢锁成功；false 表示短时间内已有同一用户在操作该帖子。
		ok, err := r.data.cache.SetNX(ctx, key, token, 3*time.Second).Result()
		if err != nil {
			return func() {}, err
		}
		if ok {
			return func() {
				const unlockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`
				_ = r.data.cache.Eval(context.Background(), unlockScript, []string{key}, token).Err()
			}, nil
		}
		if time.Now().After(deadline) {
			return func() {}, errPostLikeLockBusy
		}

		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return func() {}, ctx.Err()
		case <-timer.C:
		}
	}
}

// ============================================================
// 十二、Bloom Filter
// ============================================================

// bloomExists 判断 ID 是否可能存在。
//
// 返回 false 表示 Bloom 明确认为不存在；RedisBloom 不可用时返回 true 放行主流程。
func (r *studentRepo) bloomExists(ctx context.Context, bloomKey string, id int64) (bool, error) {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return true, nil
	}

	res, err := r.data.cache.Do(ctx, "BF.EXISTS", bloomKey, strconv.FormatInt(id, 10)).Int()
	if err != nil {
		// RedisBloom 不可用时放行，不影响主流程。
		return true, err
	}

	return res == 1, nil
}

// bloomAdd 把新创建的 ID 加入 Bloom Filter。
//
// 这是防穿透索引，不是数据缓存；写路径保留这个操作。
func (r *studentRepo) bloomAdd(ctx context.Context, bloomKey string, id int64) error {
	if r.data.cache == nil || id <= 0 || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Do(ctx, "BF.ADD", bloomKey, strconv.FormatInt(id, 10)).Err()
}

// ============================================================
// 十三、缓存 key 构造
// ============================================================

// buildStudentPostCacheKey 构造帖子对象缓存 key。
func buildStudentPostCacheKey(postID int64) string {
	return studentPostCachePrefix + strconv.FormatInt(postID, 10)
}

// buildStudentCommentCacheKey 构造评论对象缓存 key。
func buildStudentCommentCacheKey(commentID int64) string {
	return studentCommentCachePrefix + strconv.FormatInt(commentID, 10)
}

// buildPostLikeStatusCacheKey 构造用户点赞关系状态缓存 key。
// post_id 和 student_id 共同定位 post_like 唯一关系。
func buildPostLikeStatusCacheKey(postID, studentID int64) string {
	return postLikeStatusCachePrefix +
		strconv.FormatInt(postID, 10) + ":" +
		strconv.FormatInt(studentID, 10)
}

// buildStudentPostCommentListCacheKeyPrefix 构造某个帖子评论列表的删除前缀。
//
// 写评论/删评论时用该前缀 SCAN 删除该帖子所有分页缓存。
func buildStudentPostCommentListCacheKeyPrefix(postID int64) string {
	return studentPostCommentListCachePrefix + strconv.FormatInt(postID, 10) + ":"
}

// buildStudentPostCommentListCacheKey 构造学生端帖子评论列表缓存 key。
//
// key 中先放 post_id 前缀，后跟分页参数 hash，便于按 post_id 删除所有页。
func buildStudentPostCommentListCacheKey(postID int64, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"post_id":   postID,
		"page_num":  pageNum,
		"page_size": pageSize,
	}

	return buildStudentPostCommentListCacheKeyPrefix(postID) + hashStudentCacheParam(param)
}

// buildStudentMyCommentListCacheKeyPrefix 构造某个学生“我的评论”列表删除前缀。
func buildStudentMyCommentListCacheKeyPrefix(studentID int64) string {
	return studentMyCommentListCachePrefix + strconv.FormatInt(studentID, 10) + ":"
}

// buildStudentMyCommentListCacheKey 构造学生“我的评论”分页缓存 key。
func buildStudentMyCommentListCacheKey(studentID int64, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"student_id": studentID,
		"page_num":   pageNum,
		"page_size":  pageSize,
	}

	return buildStudentMyCommentListCacheKeyPrefix(studentID) + hashStudentCacheParam(param)
}

// hashStudentCacheParam 将查询参数转成稳定 hash。
//
// 返回值用于缩短 Redis key，同时避免参数顺序导致 key 不一致。
func hashStudentCacheParam(v interface{}) string {
	data, _ := json.Marshal(v)
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}

// ============================================================
// 十四、分页工具
// ============================================================

// normalizeStudentPage 统一修正学生端分页参数。
//
// 返回合法 pageNum/pageSize，pageSize 最大 100。
func normalizeStudentPage(pageNum, pageSize int32) (int32, int32) {
	if pageNum <= 0 {
		pageNum = 1
	}

	if pageSize <= 0 {
		pageSize = 5
	}

	if pageSize > 100 {
		pageSize = 100
	}

	return pageNum, pageSize
}
