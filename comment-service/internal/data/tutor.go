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
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

// 助教端缓存 key、TTL 和 Bloom Filter 相关常量。
//
// 助教端读写帖子、评论和回复，缓存类型同时覆盖对象缓存、列表缓存和空值缓存。
const (
	// MySQL 对象缓存。
	//
	// 这类缓存用于按 ID 查询：
	// mysql:post:{post_id}
	// mysql:comment:{comment_id}
	// mysql:reply:{reply_id}
	tutorPostCachePrefix    = "mysql:post:"
	tutorCommentCachePrefix = "mysql:comment:"
	tutorReplyCachePrefix   = "mysql:reply:"

	// MySQL 列表缓存。
	//
	// 这类缓存用于分页列表：
	// mysql:tutor_posts:{hash}
	// mysql:post_comments:{post_id}:{hash}
	tutorPostListCachePrefix    = "mysql:tutor_posts:"
	tutorCommentListCachePrefix = "mysql:post_comments:"

	// Bloom Filter key。
	//
	// 用于拦截明显不存在的 ID，防止缓存穿透。
	tutorBloomPostKey    = "bf:post"
	tutorBloomCommentKey = "bf:comment"

	// 缓存 TTL。
	//
	// 对象缓存可以稍长；
	// 列表缓存容易受新增/删除影响，所以建议短一点；
	// 空值缓存用于防止不存在 ID 反复打 MySQL。
	tutorObjectCacheTTL = 10 * time.Minute
	tutorListCacheTTL   = 2 * time.Minute
	tutorNullCacheTTL   = 30 * time.Second

	tutorPostListCacheVersion    = 2
	tutorCommentListCacheVersion = 2

	tutorNullCacheValue = "__nil__"
)

// tutorRepo 实现助教端的数据访问能力。
//
// 它封装帖子写入、评论删除、回复管理以及助教视角的缓存读写策略。
type tutorRepo struct {
	data *Data
	log  *log.Helper

	// mysqlGroup 用于防止 MySQL 缓存击穿。
	//
	// 同一个 post_id/comment_id/reply_id 或同一个列表查询缓存失效时，
	// 只允许一个 goroutine 真正查询 MySQL，其余请求复用结果。
	mysqlGroup singleflight.Group
}

// NewTutorRepo 创建助教端数据仓储。
func NewTutorRepo(data *Data, logger log.Logger) biz.TutorRepo {
	return &tutorRepo{
		data: data,
		log:  log.NewHelper(log.With(logger, "module", "data/tutor")),
	}
}

// ============================================================
// 一、知识帖写操作
// ============================================================

// CreatePost 创建知识帖。
//
// 缓存处理：
// 1. 创建成功后，把 post_id 加入 Bloom Filter；
// 2. 不强制刷新帖子列表缓存，帖子列表缓存依赖短 TTL 自动过期。
func (r *tutorRepo) CreatePost(ctx context.Context, post *model.Post) (*model.Post, error) {
	err := r.data.q.Transaction(func(tx *query.Query) error {
		p := tx.Post
		// post 表只保存低频核心字段；计数初始化写入 post_counter，避免后续点赞/评论更新污染 post binlog。
		if err := p.WithContext(ctx).Create(post); err != nil {
			return err
		}
		return p.WithContext(ctx).UnderlyingDB().
			Exec("INSERT IGNORE INTO post_counter (post_id, like_count, comment_count) VALUES (?, 0, 0)", post.PostID).
			Error
	})
	if err != nil {
		return nil, err
	}

	_ = r.bloomAdd(ctx, tutorBloomPostKey, post.PostID)

	return post, nil
}

// UpdatePost 更新知识帖标题和内容。
//
// 缓存处理：
// 1. 更新成功后只删除 mysql:post:core:{post_id}；
// 2. 帖子列表缓存依赖短 TTL 自动过期。
func (r *tutorRepo) UpdatePost(ctx context.Context, post *model.Post) error {
	p := r.data.q.Post

	_, err := p.WithContext(ctx).
		Where(p.PostID.Eq(post.PostID)).
		Select(p.Title, p.Content).
		Updates(post)
	if err != nil {
		return err
	}

	_ = r.delCache(ctx, buildPostCoreCacheKey(post.PostID))

	return nil
}

// DeletePost 软删除知识帖。
//
// 业务处理：
// 1. status 置为 2；
// 2. 设置 deleted_at。
//
// 缓存处理：
// 1. 删除帖子主体和统计缓存；
// 2. 不主动写入空值缓存。空值缓存只在读路径确认 DB 不存在后回填。
func (r *tutorRepo) DeletePost(ctx context.Context, postID int64) error {
	p := r.data.q.Post

	now := time.Now()
	_, err := p.WithContext(ctx).
		Where(p.PostID.Eq(postID)).
		Updates(map[string]interface{}{
			"status":     2,
			"deleted_at": now,
		})
	if err != nil {
		return err
	}

	_ = r.delCache(ctx, buildPostCoreCacheKey(postID), buildPostStatsCacheKey(postID))

	return nil
}

// ============================================================
// 二、知识帖读操作
// ============================================================

// GetPostByID 根据 post_id 查询已发布知识帖。
//
// 查询链路：
// Bloom Filter
//
//	↓
//
// Redis 缓存
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
func (r *tutorRepo) GetPostByID(ctx context.Context, postID int64) (*model.Post, error) {
	if postID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// Bloom Filter 是第一道防穿透保护。
	// false 表示该 post_id 一定不存在，可以直接返回；RedisBloom 异常时会放行到后续缓存/DB 链路。
	exists, err := r.bloomExists(ctx, tutorBloomPostKey, postID)
	if err != nil {
		r.log.WithContext(ctx).Warnf("bloom post exists failed, post_id=%d, err=%v", postID, err)
	}
	if err == nil && !exists {
		return nil, gorm.ErrRecordNotFound
	}

	post, err := loadPostWithSplitCache(
		ctx,
		r.data.cache,
		&r.mysqlGroup,
		postID,
		tutorNullCacheValue,
		tutorObjectCacheTTL,
		tutorObjectCacheTTL,
		tutorNullCacheTTL,
		r.queryPostByIDFromDB,
		r.queryPostStatsByIDFromDB,
	)
	if err != nil {
		return nil, err
	}
	// 运营端可能缓存下架帖子；助教端命中共享缓存后仍只允许读取已发布、未删除帖子。
	if post.Status != 1 || post.DeletedAt.Valid {
		return nil, gorm.ErrRecordNotFound
	}

	_ = r.bloomAdd(ctx, tutorBloomPostKey, postID)
	r.data.applyPendingPostLikeCountDelta(ctx, post)
	r.data.applyPendingPostCommentCountDelta(ctx, post)
	return post, nil
}

// ListTutorPosts 查询助教发布的知识帖列表。
//
// 查询链路：
// Redis 列表 ID 缓存
//
//	↓
//
// Redis MGET 帖子 core 对象缓存
//
//	↓
//
// singleflight + MySQL 分页查询或按 ID 批量补 miss。
//
// 注意：
// 1. 列表缓存不使用 Bloom Filter；
// 2. 列表只保存 post_id 和 total，帖子内容变更只需要删除 core 缓存；
// 3. 列表受新增、删除影响较多，使用短 TTL 自动过期。
func (r *tutorRepo) ListTutorPosts(ctx context.Context, tutorID int64, pageNum, pageSize int32) ([]*model.Post, int64, error) {
	// 分页参数先统一修正，保证 Redis key 和 MySQL 查询使用同一套分页语义。
	pageNum, pageSize = normalizeTutorPage(pageNum, pageSize)

	// 助教帖子列表 key 由 tutor_id + page 参数 hash 得到，对应一个固定分页窗口。
	key := buildTutorPostListCacheKey(tutorID, pageNum, pageSize)

	// 先读列表缓存。该缓存只保存当前页 post_id 顺序和 total，命中后再批量加载帖子对象。
	cache, hit, err := r.getPostListFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get tutor post list cache failed, key=%s, err=%v", key, err)
	} else if hit {
		posts, err := r.loadPostsByIDs(ctx, cache.PostIDs)
		return posts, cache.Total, err
	}

	// 未命中时用 singleflight 防止同一分页列表被并发回源。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// 进入 singleflight 后再次检查缓存，避免等待期间其他请求已经重建列表。
		cache, hit, err := r.getPostListFromCache(ctx, key)
		if err == nil && hit {
			return cache, nil
		}

		// 真正回源 MySQL，返回当前页帖子和 total。
		posts, total, err := r.queryTutorPostsFromDB(ctx, tutorID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// 列表缓存只写 post_id 顺序和 total；完整帖子对象交给 core/stats 缓存复用。
		writePostCoreCaches(ctx, r.data.cache, posts, tutorObjectCacheTTL)
		cache = newTutorPostListCache(posts, total)

		data, err := json.Marshal(cache)
		if err == nil {
			_ = r.setCache(ctx, key, data, tutorListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// 类型断言保证 singleflight 内部返回的是预期的列表缓存结构。
	cache, ok := val.(*postListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid tutor post list cache result")
	}

	posts, err := r.loadPostsByIDs(ctx, cache.PostIDs)
	return posts, cache.Total, err
}

// ============================================================
// 三、评论写操作
// ============================================================

// DeleteComment 软删除评论。
//
// 缓存处理：
// 1. 删除 mysql:comment:{comment_id}；
// 2. 删除相关列表 ID 缓存，下次查询回源重建。
//
// 注意：写路径只删除缓存，不主动写入 __nil__ 空值缓存。
// 空值缓存只在读路径确认 DB 不存在后回填。
func (r *tutorRepo) DeleteComment(ctx context.Context, commentID int64) error {
	var postID int64
	var studentID int64
	var auditStatus int32
	var needDecrement bool

	err := r.data.q.Transaction(func(tx *query.Query) error {
		c := tx.StudyComment

		// 助教删除评论和学生删除评论共用同一套并发语义：
		// 只有原本未删除、且本次条件更新成功的评论，才允许扣减帖子评论数。
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

		// deleted_at IS NULL 是重复删除的最后一道保护。
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

	key := buildTutorCommentCacheKey(commentID)
	_ = r.delCache(ctx, key)
	if err := deleteCommentListCaches(ctx, r.data.cache, postID, studentID, auditStatus); err != nil {
		r.log.WithContext(ctx).Warnf("delete comment list caches after tutor delete comment failed, comment_id=%d, post_id=%d, student_id=%d, err=%v", commentID, postID, studentID, err)
	}

	return nil
}

// ============================================================
// 四、评论读操作
// ============================================================

// ListPostComments 查询某个知识帖下的可见评论列表。
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
func (r *tutorRepo) ListPostComments(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	// 统一分页，避免同一请求因为非法页码生成不同缓存 key。
	pageNum, pageSize = normalizeTutorPage(pageNum, pageSize)

	// 评论列表缓存只描述“某个帖子 + 某一页”的 comment_id 顺序和 total。
	key := buildTutorCommentListCacheKey(postID, pageNum, pageSize)

	// 第一层读取列表 ID 缓存。
	// 命中后还需要批量加载评论对象，因为完整评论内容放在对象缓存中复用。
	cache, hit, err := r.getCommentListFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get post comment list cache failed, key=%s, err=%v", key, err)
	} else if hit {
		comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
		return comments, cache.Total, err
	}

	// 列表未命中时使用 singleflight 合并同一页评论的回源查询。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 可以复用其他请求刚写入的列表缓存。
		cache, hit, err := r.getCommentListFromCache(ctx, key)
		if err == nil && hit {
			return cache, nil
		}

		// 回源 MySQL 查询当前页可见评论和 total。
		comments, total, err := r.queryPostCommentsFromDB(ctx, postID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// 评论对象单独回填对象缓存，列表缓存只保存 ID，后续详情页也能复用对象缓存。
		r.setCommentObjectCaches(ctx, comments)

		// 根据当前页结果重建列表 ID 缓存。
		cache = newTutorCommentListCache(comments, total)

		data, err := json.Marshal(cache)
		if err == nil {
			_ = r.setCache(ctx, key, data, tutorListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// singleflight 返回列表缓存结构后，再按 ID 加载对象，保持与缓存中的 ID 顺序一致。
	cache, ok := val.(*commentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid post comment list cache result")
	}

	comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryVisibleCommentsByIDsFromDB)
	return comments, cache.Total, err
}

// GetCommentByID 根据 comment_id 查询评论。
//
// 查询链路：
// Bloom Filter
//
//	↓
//
// Redis 缓存
//
//	↓
//
// singleflight
//
//	↓
//
// MySQL
func (r *tutorRepo) GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	if commentID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// Bloom Filter 用于拦截明显不存在的 comment_id，减少无效 ID 对 Redis/MySQL 的压力。
	exists, err := r.bloomExists(ctx, tutorBloomCommentKey, commentID)
	if err != nil {
		r.log.WithContext(ctx).Warnf("bloom comment exists failed, comment_id=%d, err=%v", commentID, err)
	}
	if err == nil && !exists {
		return nil, gorm.ErrRecordNotFound
	}

	key := buildTutorCommentCacheKey(commentID)

	// 先读评论对象缓存；命中空值缓存时直接返回 not found。
	comment, hit, err := r.getCommentFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get comment cache failed, key=%s, err=%v", key, err)
	} else if hit {
		if comment == nil {
			return nil, gorm.ErrRecordNotFound
		}
		return comment, nil
	}

	// 缓存未命中后合并相同 comment_id 的 MySQL 回源请求。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 防止等待 singleflight 时缓存已被其他请求写入。
		comment, hit, err := r.getCommentFromCache(ctx, key)
		if err == nil && hit {
			return comment, nil
		}

		// 回源 MySQL；不存在时写短 TTL 空值缓存，存在时写对象缓存并补 Bloom。
		comment, err = r.queryCommentByIDFromDB(ctx, commentID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = r.setCache(ctx, key, []byte(tutorNullCacheValue), tutorNullCacheTTL)
			}
			return nil, err
		}

		data, err := json.Marshal(comment)
		if err == nil {
			_ = r.setCache(ctx, key, data, tutorObjectCacheTTL)
		}

		_ = r.bloomAdd(ctx, tutorBloomCommentKey, commentID)

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

// ============================================================
// 五、回复写操作
// ============================================================

// CreateCommentReply 将一次性助教回复直接写入 study_comment。
// reply_status=0 是并发条件，确保一条评论最多回复一次。
func (r *tutorRepo) CreateCommentReply(ctx context.Context, reply *model.StudyCommentReply) (*model.StudyCommentReply, error) {
	result := r.data.q.StudyComment.WithContext(ctx).UnderlyingDB().Exec(`
UPDATE study_comment
SET reply_id = ?,
	reply_tutor_id = ?,
	reply_content = ?,
	reply_status = 1,
	replied_at = CURRENT_TIMESTAMP
WHERE comment_id = ?
  AND reply_status = 0
  AND deleted_at IS NULL
`, reply.CommentReplyID, reply.TutorID, reply.Content, reply.CommentID)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, biz.ErrCommentAlreadyReplied
	}

	_ = r.delCache(ctx, buildTutorCommentCacheKey(reply.CommentID))
	return reply, nil
}

// DeleteReply 撤回评论行中内嵌的一次性助教回复。
func (r *tutorRepo) DeleteReply(ctx context.Context, replyID, deletedBy int64) error {
	reply, err := r.GetReplyByID(ctx, replyID)
	if err != nil {
		return err
	}

	result := r.data.q.StudyComment.WithContext(ctx).UnderlyingDB().Exec(`
UPDATE study_comment
SET reply_status = 2
WHERE reply_id = ?
  AND reply_tutor_id = ?
  AND reply_status = 1
  AND deleted_at IS NULL
`, replyID, deletedBy)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("回复不存在、已撤回或无权操作")
	}

	_ = r.delCache(ctx, buildTutorReplyCacheKey(replyID))
	_ = r.delCache(ctx, buildTutorCommentCacheKey(reply.CommentID))
	return nil
}

// ============================================================
// 六、回复读操作
// ============================================================

// GetReplyByID 根据 reply_id 查询单条回复。
//
// 查询链路：
// Redis 缓存
//
//	↓
//
// singleflight
//
//	↓
//
// MySQL
func (r *tutorRepo) GetReplyByID(ctx context.Context, replyID int64) (*model.StudyCommentReply, error) {
	if replyID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	key := buildTutorReplyCacheKey(replyID)

	reply, hit, err := r.getReplyFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get reply cache failed, key=%s, err=%v", key, err)
	} else if hit {
		if reply == nil {
			return nil, gorm.ErrRecordNotFound
		}
		return reply, nil
	}

	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		reply, hit, err := r.getReplyFromCache(ctx, key)
		if err == nil && hit {
			return reply, nil
		}

		reply, err = r.queryReplyByIDFromDB(ctx, replyID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = r.setCache(ctx, key, []byte(tutorNullCacheValue), tutorNullCacheTTL)
			}
			return nil, err
		}

		data, err := json.Marshal(reply)
		if err == nil {
			_ = r.setCache(ctx, key, data, tutorObjectCacheTTL)
		}

		return reply, nil
	})
	if err != nil {
		return nil, err
	}

	reply, ok := val.(*model.StudyCommentReply)
	if !ok || reply == nil {
		return nil, gorm.ErrRecordNotFound
	}

	return reply, nil
}

// ============================================================
// 七、聚合查询
// ============================================================

// GetPostDetailWithComments 查询帖子详情及其评论列表。
//
// 复用已有缓存方法：
// 1. GetPostByID 负责帖子对象缓存；
// 2. ListPostComments 负责评论列表 ID 缓存和评论对象 MGET；
// 3. 该方法本身不再直接查 MySQL。
func (r *tutorRepo) GetPostDetailWithComments(
	ctx context.Context,
	postID int64,
	pageNum, pageSize int32,
) (*model.Post, []*model.StudyComment, int64, error) {
	post, err := r.GetPostByID(ctx, postID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, 0, errors.New("帖子不存在或已下架")
		}
		return nil, nil, 0, err
	}

	comments, total, err := r.ListPostComments(ctx, postID, pageNum, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}

	return post, comments, total, nil
}

// ============================================================
// 八、MySQL 原始查询方法
//
// 说明：
// 1. 这些方法只负责查 DB；
// 2. 不读写缓存；
// 3. 外层方法负责 Redis、Bloom、singleflight。
// ============================================================

// queryPostByIDFromDB 从 MySQL 查询单个已发布且未删除的帖子。
//
// 返回值：
// 1. *model.Post：命中的帖子记录；
// 2. error：gorm 返回的查询错误，包含 record not found。
func (r *tutorRepo) queryPostByIDFromDB(ctx context.Context, postID int64) (*model.Post, error) {
	p := r.data.q.Post

	post, err := p.WithContext(ctx).
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

func (r *tutorRepo) queryPostStatsByIDFromDB(ctx context.Context, postID int64) (*postStatsCache, error) {
	stats, err := r.data.queryPostCounterByID(ctx, postID)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// queryTutorPostsFromDB 从 MySQL 分页查询某个助教发布的帖子。
//
// FindByPage 返回：
// 1. 当前页帖子列表；
// 2. 满足条件的总数，用于前端分页；
// 3. 查询错误。
func (r *tutorRepo) queryTutorPostsFromDB(ctx context.Context, tutorID int64, pageNum, pageSize int32) ([]*model.Post, int64, error) {
	p := r.data.q.Post

	// offset 是 MySQL LIMIT/OFFSET 分页起点。
	offset := int((pageNum - 1) * pageSize)

	posts, total, err := p.WithContext(ctx).
		Where(p.AuthorID.Eq(tutorID), p.Status.Eq(1), p.DeletedAt.IsNull()).
		Order(p.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
	if err != nil {
		return nil, 0, err
	}
	if err := r.data.attachPostCounters(ctx, posts); err != nil {
		return nil, 0, err
	}
	return posts, total, nil
}

// queryVisiblePostsByIDsFromDB 批量查询仍然发布且未删除的帖子。
//
// 入参通常来自帖子列表 ID 缓存；返回值会在 loadPostsByIDs 中按原 ID 顺序重新组装。
func (r *tutorRepo) queryVisiblePostsByIDsFromDB(ctx context.Context, postIDs []int64) ([]*model.Post, error) {
	if len(postIDs) == 0 {
		return []*model.Post{}, nil
	}

	p := r.data.q.Post
	return p.WithContext(ctx).
		Where(p.PostID.In(postIDs...), p.Status.Eq(1), p.DeletedAt.IsNull()).
		Find()
}

// queryCommentByIDFromDB 从 MySQL 查询单条未删除评论。
//
// 助教端需要用该方法做评论详情、删除权限校验、回复前校验等强一致读取。
func (r *tutorRepo) queryCommentByIDFromDB(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
		First()
}

// queryVisibleCommentsByIDsFromDB 按 comment_id 批量查询可见评论。
//
// 入参 commentIDs 来自评论列表 ID 缓存；返回值只包含仍然可见且未删除的评论对象。
func (r *tutorRepo) queryVisibleCommentsByIDsFromDB(ctx context.Context, commentIDs []int64) ([]*model.StudyComment, error) {
	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.In(commentIDs...), c.VisibleStatus.Eq(1), c.DeletedAt.IsNull()).
		Find()
}

// queryPostCommentsFromDB 从 MySQL 分页查询帖子下的可见评论。
//
// 返回 comments 和 total，外层会把 comments 拆成 comment_id 列表缓存，
// 再把每条评论写入对象缓存，方便后续不同列表复用。
func (r *tutorRepo) queryPostCommentsFromDB(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	c := r.data.q.StudyComment

	// offset 控制当前页起点，排序使用 created_at desc 保证最新评论优先展示。
	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.PostID.Eq(postID), c.VisibleStatus.Eq(1), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
}

// queryReplyByIDFromDB 从 study_comment 内嵌回复字段查询单条有效回复。
func (r *tutorRepo) queryReplyByIDFromDB(ctx context.Context, replyID int64) (*model.StudyCommentReply, error) {
	var comment model.StudyComment
	err := r.data.q.StudyComment.WithContext(ctx).UnderlyingDB().
		Raw(`
SELECT *
FROM study_comment
WHERE reply_id = ?
  AND reply_status = 1
  AND deleted_at IS NULL
LIMIT 1
`, replyID).
		Scan(&comment).Error
	if err != nil {
		return nil, err
	}
	if comment.CommentID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}
	return embeddedReplyFromComment(&comment), nil
}

func embeddedReplyFromComment(comment *model.StudyComment) *model.StudyCommentReply {
	if comment == nil || comment.ReplyStatus != 1 || comment.ReplyID <= 0 || comment.ReplyContent == nil {
		return nil
	}
	reply := &model.StudyCommentReply{
		CommentReplyID: comment.ReplyID,
		CommentID:      comment.CommentID,
		TutorID:        comment.ReplyTutorID,
		PostID:         comment.PostID,
		Content:        *comment.ReplyContent,
		Status:         1,
	}
	if comment.RepliedAt != nil {
		reply.CreatedAt = *comment.RepliedAt
	}
	return reply
}

// ============================================================
// 九、缓存结构体
// ============================================================

// postListCache 是助教帖子列表缓存 value。
//
// 列表只保存 post_id 顺序和 total；完整帖子对象走 mysql:post:core:{post_id}
// 与 mysql:post:stats:{post_id}，这样单个帖子编辑时不用清理所有列表页缓存。
type postListCache struct {
	Version int     `json:"version"`
	PostIDs []int64 `json:"post_ids"`
	Total   int64   `json:"total"`
}

// commentListCache 是助教端帖子评论列表缓存 value。
//
// 只保存 comment_id 顺序和 total，完整评论对象仍走 mysql:comment:{comment_id} 对象缓存。
type commentListCache struct {
	Version    int     `json:"version"`
	CommentIDs []int64 `json:"comment_ids"`
	Total      int64   `json:"total"`
}

func newTutorPostListCache(posts []*model.Post, total int64) *postListCache {
	return &postListCache{
		Version: tutorPostListCacheVersion,
		PostIDs: collectTutorPostIDs(posts),
		Total:   total,
	}
}

func collectTutorPostIDs(posts []*model.Post) []int64 {
	ids := make([]int64, 0, len(posts))
	for _, post := range posts {
		if post == nil || post.PostID <= 0 {
			continue
		}
		ids = append(ids, post.PostID)
	}
	return ids
}

// newTutorCommentListCache 把评论对象列表转换成轻量列表缓存。
//
// value 只保存 comment_id 列表和 total，不保存完整评论内容；
// 下次读取列表时再通过 loadCommentsByIDs 读取评论对象缓存或回源 MySQL。
func newTutorCommentListCache(comments []*model.StudyComment, total int64) *commentListCache {
	return &commentListCache{
		Version:    tutorCommentListCacheVersion,
		CommentIDs: collectTutorCommentIDs(comments),
		Total:      total,
	}
}

// collectTutorCommentIDs 从评论对象中提取有效 comment_id。
//
// 返回值顺序和 comments 顺序一致，因此列表缓存可以保留数据库排序结果。
func collectTutorCommentIDs(comments []*model.StudyComment) []int64 {
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
// 十、对象缓存读取方法
// ============================================================

// getPostFromCache 读取帖子对象缓存。
//
// 返回值含义：
// 1. post：缓存中的帖子对象；
// 2. hit：true 表示 Redis 命中，包含空值缓存命中；
// 3. error：Redis 或 JSON 解析错误。
func (r *tutorRepo) getPostFromCache(ctx context.Context, key string) (*model.Post, bool, error) {
	// getCache 返回原始 Redis bytes；len(data)==0 表示未命中。
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == tutorNullCacheValue {
		// "__nil__" 是空值缓存，表示 DB 已确认不存在，用于防止缓存穿透。
		return nil, true, nil
	}

	var post model.Post
	if err := json.Unmarshal(data, &post); err != nil {
		// 缓存内容损坏时删除，避免后续请求反复解析失败。
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &post, true, nil
}

// getCommentFromCache 读取评论对象缓存。
//
// 返回值和 getPostFromCache 一致：comment 是对象，hit 表示是否命中，error 表示异常。
func (r *tutorRepo) getCommentFromCache(ctx context.Context, key string) (*model.StudyComment, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == tutorNullCacheValue {
		// 空值缓存命中时不再回源 MySQL。
		return nil, true, nil
	}

	var comment model.StudyComment
	if err := json.Unmarshal(data, &comment); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &comment, true, nil
}

// getReplyFromCache 读取回复对象缓存。
//
// 当前主要用于按 reply_id 删除回复前做强一致辅助读取。
func (r *tutorRepo) getReplyFromCache(ctx context.Context, key string) (*model.StudyCommentReply, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == tutorNullCacheValue {
		return nil, true, nil
	}

	var reply model.StudyCommentReply
	if err := json.Unmarshal(data, &reply); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &reply, true, nil
}

// loadCommentsByIDs 按 comment_id 顺序批量加载评论对象。
//
// 调用链：
// 1. getCommentsFromObjectCache 先 MGET 评论对象缓存，返回 cached map 和 missed IDs；
// 2. queryMissed 只查询缓存未命中的评论；
// 3. setCommentObjectCaches 把回源评论写入对象缓存；
// 4. 最后按 commentIDs 原顺序组装结果返回。
func (r *tutorRepo) loadCommentsByIDs(ctx context.Context, commentIDs []int64, queryMissed func(context.Context, []int64) ([]*model.StudyComment, error)) ([]*model.StudyComment, error) {
	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	// cached 保存已命中的 comment_id -> comment，missed 是需要回源 MySQL 的 ID。
	cached, missed, err := r.getCommentsFromObjectCache(ctx, commentIDs)
	if err != nil {
		r.log.WithContext(ctx).Warnf("mget tutor comment object cache failed, err=%v", err)
		missed = commentIDs
	}

	if len(missed) > 0 {
		// queryMissed 返回仍然可见/未删除的评论对象，具体过滤条件由调用方传入。
		comments, err := queryMissed(ctx, missed)
		if err != nil {
			return nil, err
		}
		// 读路径回源后再写对象缓存；写路径不会主动加载缓存。
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
// 返回值：
// 1. map[int64]*StudyComment：命中的评论对象；
// 2. []int64：未命中的 comment_id；
// 3. error：Redis MGET 错误。
func (r *tutorRepo) getCommentsFromObjectCache(ctx context.Context, commentIDs []int64) (map[int64]*model.StudyComment, []int64, error) {
	result := make(map[int64]*model.StudyComment, len(commentIDs))
	missed := make([]int64, 0, len(commentIDs))
	if len(commentIDs) == 0 {
		return result, missed, nil
	}
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return result, commentIDs, nil
	}

	keys := make([]string, 0, len(commentIDs))
	for _, commentID := range commentIDs {
		keys = append(keys, buildTutorCommentCacheKey(commentID))
	}

	// MGET 一次拿多条评论，减少 Redis 往返次数；span 会记录命中和未命中数量。
	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.MGET mysql:comment",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "MGET"),
		attribute.String("app.role", "tutor"),
		attribute.String("cache.key.prefix", tutorCommentCachePrefix),
		attribute.Int("cache.keys.count", len(keys)),
	)
	values, err := r.data.cache.MGet(cacheCtx, keys...).Result()
	if err != nil {
		observability.EndSpan(cacheSpan, err)
		return result, commentIDs, err
	}

	for i, value := range values {
		commentID := commentIDs[i]
		if value == nil {
			// Redis nil 表示该评论对象缓存未命中，需要回源。
			missed = append(missed, commentID)
			continue
		}

		// redisValueBytes 兼容 go-redis 返回 string 或 []byte 两种类型。
		raw, ok := redisValueBytes(value)
		if !ok {
			missed = append(missed, commentID)
			continue
		}
		if string(raw) == tutorNullCacheValue {
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

// setCommentObjectCaches 将评论对象写入 Redis 对象缓存并补充 Bloom。
//
// 只在读路径回源 MySQL 后调用；新增/删除/审核这类写路径只删除缓存，不主动写入列表缓存。
func (r *tutorRepo) setCommentObjectCaches(ctx context.Context, comments []*model.StudyComment) {
	for _, comment := range comments {
		if comment == nil || comment.CommentID <= 0 {
			continue
		}
		if data, err := json.Marshal(comment); err == nil {
			_ = r.setCache(ctx, buildTutorCommentCacheKey(comment.CommentID), data, tutorObjectCacheTTL)
		}
		_ = r.bloomAdd(ctx, tutorBloomCommentKey, comment.CommentID)
	}
}

// loadPostsByIDs 按 post_id 顺序批量加载帖子对象。
//
// 列表缓存只保存 ID，这里先 MGET core 对象缓存；miss 的帖子再用 MySQL IN 批量回源，
// 最后统一挂载 post_counter 与 Redis 未落库 delta。
func (r *tutorRepo) loadPostsByIDs(ctx context.Context, postIDs []int64) ([]*model.Post, error) {
	if len(postIDs) == 0 {
		return []*model.Post{}, nil
	}

	cached, missed, err := r.getPostsFromCoreCache(ctx, postIDs)
	if err != nil {
		r.log.WithContext(ctx).Warnf("mget tutor post core cache failed, err=%v", err)
		missed = postIDs
	}

	if len(missed) > 0 {
		posts, err := r.queryVisiblePostsByIDsFromDB(ctx, missed)
		if err != nil {
			return nil, err
		}
		writePostCoreCaches(ctx, r.data.cache, posts, tutorObjectCacheTTL)
		for _, post := range posts {
			if post == nil {
				continue
			}
			cached[post.PostID] = post
		}
	}

	result := make([]*model.Post, 0, len(postIDs))
	for _, postID := range postIDs {
		if post := cached[postID]; post != nil {
			result = append(result, post)
		}
	}
	if err := r.data.attachPostCounters(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *tutorRepo) getPostsFromCoreCache(ctx context.Context, postIDs []int64) (map[int64]*model.Post, []int64, error) {
	result := make(map[int64]*model.Post, len(postIDs))
	missed := make([]int64, 0, len(postIDs))
	if len(postIDs) == 0 {
		return result, missed, nil
	}
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return result, postIDs, nil
	}

	keys := make([]string, 0, len(postIDs))
	for _, postID := range postIDs {
		keys = append(keys, buildPostCoreCacheKey(postID))
	}

	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.MGET mysql:post:core",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "MGET"),
		attribute.String("app.role", "tutor"),
		attribute.String("cache.key.prefix", postCoreCachePrefix),
		attribute.Int("cache.keys.count", len(keys)),
	)
	values, err := r.data.cache.MGet(cacheCtx, keys...).Result()
	if err != nil {
		observability.EndSpan(cacheSpan, err)
		return result, postIDs, err
	}

	for i, value := range values {
		postID := postIDs[i]
		if value == nil {
			missed = append(missed, postID)
			continue
		}
		raw, ok := redisValueBytes(value)
		if !ok || string(raw) == tutorNullCacheValue {
			missed = append(missed, postID)
			continue
		}

		var post model.Post
		if err := json.Unmarshal(raw, &post); err != nil {
			_ = r.delCache(ctx, keys[i])
			missed = append(missed, postID)
			continue
		}
		if post.PostID <= 0 {
			_ = r.delCache(ctx, keys[i])
			missed = append(missed, postID)
			continue
		}
		result[post.PostID] = &post
	}

	cacheSpan.SetAttributes(
		attribute.Int("cache.hit.count", len(result)),
		attribute.Int("cache.miss.count", len(missed)),
	)
	observability.EndSpan(cacheSpan, nil)
	return result, missed, nil
}

// ============================================================
// 十一、列表缓存读取方法
// ============================================================

// getPostListFromCache 读取助教帖子分页列表 ID 缓存。
//
// 返回 postListCache，其中 PostIDs 是当前页帖子 ID 顺序，Total 是总数。
func (r *tutorRepo) getPostListFromCache(ctx context.Context, key string) (*postListCache, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var cache postListCache
	if err := json.Unmarshal(data, &cache); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}
	if cache.Version != tutorPostListCacheVersion {
		_ = r.delCache(ctx, key)
		return nil, false, nil
	}
	if cache.PostIDs == nil {
		cache.PostIDs = []int64{}
	}

	return &cache, true, nil
}

// getCommentListFromCache 读取帖子评论列表 ID 缓存。
//
// 返回 commentListCache，其中 CommentIDs 是当前页评论 ID，Total 是总评论数；
// 外层还需要调用 loadCommentsByIDs 才能拿到完整评论对象。
func (r *tutorRepo) getCommentListFromCache(ctx context.Context, key string) (*commentListCache, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var cache commentListCache
	if err := json.Unmarshal(data, &cache); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}
	if cache.Version != tutorCommentListCacheVersion {
		// 版本不一致说明缓存结构已经升级，删除后让下一次读取重建。
		_ = r.delCache(ctx, key)
		return nil, false, nil
	}
	if cache.CommentIDs == nil {
		cache.CommentIDs = []int64{}
	}

	return &cache, true, nil
}

// ============================================================
// 十二、Redis 基础操作
// ============================================================

// getCache 封装 Redis GET。
//
// 返回 nil,nil 表示缓存未命中、Redis 未配置或本次请求开启了 cache bypass。
func (r *tutorRepo) getCache(ctx context.Context, key string) ([]byte, error) {
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

// setCache 封装 Redis SET，并统一给 TTL 增加随机抖动。
//
// 返回 error 仅表示 Redis 写入失败；调用方通常记录日志但不影响主流程。
func (r *tutorRepo) setCache(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Set(ctx, key, data, cacheTTLWithJitter(ttl)).Err()
}

// delCache 删除一个或多个 Redis key。
//
// 写路径调用该方法做缓存失效，删除失败会返回 error 给外层记录。
func (r *tutorRepo) delCache(ctx context.Context, keys ...string) error {
	if r.data.cache == nil || len(keys) == 0 {
		return nil
	}

	return r.data.cache.Del(ctx, keys...).Err()
}

// ============================================================
// 十三、Bloom Filter
// ============================================================

// bloomExists 查询 RedisBloom 中是否可能存在指定 ID。
//
// 返回 true 表示“可能存在”，false 表示“一定不存在”；RedisBloom 不可用时返回 true 放行。
func (r *tutorRepo) bloomExists(ctx context.Context, bloomKey string, id int64) (bool, error) {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return true, nil
	}

	res, err := r.data.cache.Do(ctx, "BF.EXISTS", bloomKey, strconv.FormatInt(id, 10)).Int()
	if err != nil {
		// RedisBloom 不可用时，选择放行，不影响主流程。
		return true, err
	}

	return res == 1, nil
}

// bloomAdd 将已创建或已读到的 ID 写入 Bloom Filter。
//
// 该操作只作为防穿透辅助索引，不承担数据一致性职责。
func (r *tutorRepo) bloomAdd(ctx context.Context, bloomKey string, id int64) error {
	if r.data.cache == nil || id <= 0 || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Do(ctx, "BF.ADD", bloomKey, strconv.FormatInt(id, 10)).Err()
}

// ============================================================
// 十四、缓存 key 构造
// ============================================================

// buildTutorPostCacheKey 生成帖子对象缓存 key，格式为 mysql:post:{post_id}。
func buildTutorPostCacheKey(postID int64) string {
	return tutorPostCachePrefix + strconv.FormatInt(postID, 10)
}

// buildTutorCommentCacheKey 生成评论对象缓存 key，格式为 mysql:comment:{comment_id}。
func buildTutorCommentCacheKey(commentID int64) string {
	return tutorCommentCachePrefix + strconv.FormatInt(commentID, 10)
}

// buildTutorReplyCacheKey 生成回复对象缓存 key，格式为 mysql:comment_reply:{reply_id}。
func buildTutorReplyCacheKey(replyID int64) string {
	return tutorReplyCachePrefix + strconv.FormatInt(replyID, 10)
}

// buildTutorPostListCacheKey 生成助教帖子列表缓存 key。
//
// key 中使用参数 hash，避免 page_num/page_size/tutor_id 拼接过长或顺序不稳定。
func buildTutorPostListCacheKey(tutorID int64, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"tutor_id":  tutorID,
		"page_num":  pageNum,
		"page_size": pageSize,
	}

	return tutorPostListCachePrefix + hashTutorCacheParam(param)
}

// buildTutorCommentListCacheKeyPrefix 生成某个 post_id 的评论列表缓存前缀。
//
// 写路径删除评论列表缓存时会按该前缀 SCAN，覆盖不同分页大小的列表缓存。
func buildTutorCommentListCacheKeyPrefix(postID int64) string {
	return tutorCommentListCachePrefix + strconv.FormatInt(postID, 10) + ":"
}

// buildTutorCommentListCacheKey 生成助教端帖子评论列表缓存 key。
//
// value 保存 comment_id 列表和 total，不保存评论完整内容。
func buildTutorCommentListCacheKey(postID int64, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"post_id":   postID,
		"page_num":  pageNum,
		"page_size": pageSize,
	}

	return buildTutorCommentListCacheKeyPrefix(postID) + hashTutorCacheParam(param)
}

// hashTutorCacheParam 将缓存参数序列化后计算 sha1，生成固定长度 key 后缀。
func hashTutorCacheParam(v interface{}) string {
	data, _ := json.Marshal(v)
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}

// ============================================================
// 十五、分页工具
// ============================================================

// normalizeTutorPage 统一修正助教端分页参数。
//
// 返回值 pageNum 至少为 1，pageSize 默认 5，最大 100，避免大分页压垮 DB。
func normalizeTutorPage(pageNum, pageSize int32) (int32, int32) {
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
