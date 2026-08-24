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

// 运营端缓存 key、TTL 和 Bloom Filter 相关常量。
//
// 运营端需要查看审核列表和完整评论上下文，因此列表缓存按审核状态和帖子维度拆分。
const (
	// 运营端复用 MySQL 对象缓存。
	//
	// 这些 key 可以和 student/tutor 端共享同一份对象缓存：
	// mysql:post:{post_id}
	// mysql:comment:{comment_id}
	operatorPostCachePrefix    = "mysql:post:"
	operatorCommentCachePrefix = "mysql:comment:"

	// 运营端列表缓存。
	//
	// 列表受审核、删除、新增影响较多，所以只做短 TTL 缓存。
	operatorAuditCommentListCachePrefix = "mysql:operator:audit_comments:"
	operatorPostCommentListCachePrefix  = "mysql:operator:post_comments:"

	// Bloom Filter key。
	//
	// Bloom 只用于按 ID 查询，拦截明显不存在的 post_id/comment_id。
	operatorBloomPostKey    = "bf:post"
	operatorBloomCommentKey = "bf:comment"

	// 缓存 TTL。
	operatorObjectCacheTTL = 10 * time.Minute
	operatorListCacheTTL   = 2 * time.Minute
	operatorNullCacheTTL   = 30 * time.Second

	operatorCommentListCacheVersion = 2

	operatorNullCacheValue = "__nil__"
)

// operatorRepo 实现运营端的数据访问能力。
//
// 它封装审核状态机写入、审核列表查询、运营视角帖子详情和缓存失效策略。
type operatorRepo struct {
	data *Data
	log  *log.Helper

	// mysqlGroup 用于防止缓存击穿。
	//
	// 同一个 comment_id/post_id 或同一个列表缓存失效时，
	// 只允许一个 goroutine 查询 MySQL，其余请求复用结果。
	mysqlGroup singleflight.Group
}

// NewOperatorRepo 创建运营端数据仓储。
func NewOperatorRepo(data *Data, logger log.Logger) biz.OperatorRepo {
	return &operatorRepo{
		data: data,
		log:  log.NewHelper(log.With(logger, "module", "data/operator")),
	}
}

// ============================================================
// 一、审核写操作
// ============================================================

// ApproveComment 审核通过。
//
// 业务处理：
// 1. audit_status 置为 1；
// 2. 记录 manual_operator_id。
//
// 缓存处理：
// 1. 删除 mysql:comment:{comment_id}；
// 2. 删除待审核和已通过列表 ID 缓存，下次查询回源重建。
func (r *operatorRepo) ApproveComment(ctx context.Context, commentID, operatorID int64) error {
	c := r.data.q.StudyComment

	// 审核通过用 audit_status=0 做条件更新，确保多个运营并发审核时只有一个成功。
	info, err := c.WithContext(ctx).
		Where(c.CommentID.Eq(commentID), c.AuditStatus.Eq(0), c.DeletedAt.IsNull()).
		Updates(map[string]interface{}{
			"audit_status":       1,
			"manual_operator_id": operatorID,
		})
	if err != nil {
		return err
	}
	if info.RowsAffected == 0 {
		return errors.New("该评论已被审核或不存在，请勿重复操作")
	}

	_ = r.delCache(ctx, buildOperatorCommentCacheKey(commentID))
	if err := deleteCommentListCaches(ctx, r.data.cache, 0, 0, 0, 1); err != nil {
		r.log.WithContext(ctx).Warnf("delete audit comment list caches after approve comment failed, comment_id=%d, err=%v", commentID, err)
	}

	return nil
}

// RejectComment 审核驳回。
//
// 事务内容：
// 1. 查询评论，用于获取 post_id 和原 visible_status；
// 2. 更新评论审核状态、可见状态、审核原因、操作人；
// 3. 如果评论原本可见，事务提交后把 comment_count=-1 写入 Redis delta。
//
// 缓存处理：
// 1. 删除 mysql:comment:{comment_id}；
// 2. 如果影响帖子评论数，则删除 mysql:post:stats:{post_id}；
// 3. 删除相关列表 ID 缓存，下次查询回源重建。
func (r *operatorRepo) RejectComment(ctx context.Context, commentID, operatorID int64, reason string) error {
	var postID int64
	var studentID int64
	var oldAuditStatus int32
	var needDeletePostCache bool

	err := r.data.q.Transaction(func(tx *query.Query) error {
		c := tx.StudyComment

		comment, err := c.WithContext(ctx).
			Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
			First()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("评论不存在")
		}
		if err != nil {
			return err
		}

		postID = comment.PostID
		studentID = comment.StudentID
		oldAuditStatus = comment.AuditStatus

		// 驳回也必须在 update 条件里带 audit_status=0，不能只依赖 biz 层先查状态。
		info, err := c.WithContext(ctx).
			Where(c.CommentID.Eq(commentID), c.AuditStatus.Eq(0), c.DeletedAt.IsNull()).
			Updates(map[string]interface{}{
				"audit_status":         2,
				"visible_status":       2,
				"manual_review_reason": reason,
				"manual_operator_id":   operatorID,
			})
		if err != nil {
			return err
		}
		if info.RowsAffected == 0 {
			return errors.New("该评论已被审核，请勿重复操作")
		}

		if comment.VisibleStatus == 1 {
			needDeletePostCache = true
		}

		return nil
	})
	if err != nil {
		return err
	}
	if needDeletePostCache {
		r.data.recordPostCommentCountDelta(ctx, postID, -1)
	}

	_ = r.delCache(ctx, buildOperatorCommentCacheKey(commentID))

	if err := deleteCommentListCaches(ctx, r.data.cache, postID, studentID, oldAuditStatus, 2); err != nil {
		r.log.WithContext(ctx).Warnf("delete comment list caches after reject comment failed, comment_id=%d, post_id=%d, student_id=%d, err=%v", commentID, postID, studentID, err)
	}

	return nil
}

// ============================================================
// 二、评论读操作
// ============================================================

// ListCommentsByAuditStatus 按审核状态查询评论列表。
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
// 1. 审核列表变化频繁，使用短 TTL；
// 2. 不使用 Bloom Filter，因为这是条件列表查询，不是按 ID 查询。
func (r *operatorRepo) ListCommentsByAuditStatus(ctx context.Context, auditStatus int32, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	// 统一分页参数，保证同一审核列表请求对应稳定的缓存 key 和 SQL offset。
	pageNum, pageSize = normalizeOperatorPage(pageNum, pageSize)

	// 审核列表缓存按 audit_status + page 参数拆分，一个 key 对应一页 comment_id 列表。
	key := buildOperatorAuditCommentListCacheKey(auditStatus, pageNum, pageSize)

	// 第一层读取审核列表 ID 缓存。
	// 命中后再批量加载评论对象，避免列表缓存存整页对象导致多个列表之间缓存重复。
	cache, hit, err := r.getCommentListFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get audit comment list cache failed, key=%s, err=%v", key, err)
	} else if hit {
		comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
		return comments, cache.Total, err
	}

	// 缓存未命中时使用 singleflight 合并同一个审核状态分页的 MySQL 回源。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 处理等待期间其他请求已经写入缓存的情况。
		cache, hit, err := r.getCommentListFromCache(ctx, key)
		if err == nil && hit {
			return cache, nil
		}

		// 真正回源 MySQL，按审核状态分页查询并返回 total。
		comments, total, err := r.queryCommentsByAuditStatusFromDB(ctx, auditStatus, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// 回填评论对象缓存，列表缓存只保存 ID 和 total。
		r.setCommentObjectCaches(ctx, comments)

		// 重建短 TTL 审核列表缓存；审核动作会删除相关状态的列表缓存。
		cache = newOperatorCommentListCache(comments, total)

		if data, err := json.Marshal(cache); err == nil {
			_ = r.setCache(ctx, key, data, operatorListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// singleflight 返回的是列表缓存结构，类型校验失败说明内部返回值被错误改动。
	cache, ok := val.(*operatorCommentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid audit comment list cache result")
	}

	// 最终按缓存中的 comment_id 顺序加载完整评论对象，并返回当前页列表和 total。
	comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
	return comments, cache.Total, err
}

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
func (r *operatorRepo) GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	if commentID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// Bloom Filter 用于快速过滤明显不存在的 comment_id；异常时放行，避免影响主链路。
	exists, err := r.bloomExists(ctx, operatorBloomCommentKey, commentID)
	if err != nil {
		r.log.WithContext(ctx).Warnf("bloom comment exists failed, comment_id=%d, err=%v", commentID, err)
	}
	if err == nil && !exists {
		return nil, gorm.ErrRecordNotFound
	}

	key := buildOperatorCommentCacheKey(commentID)

	// 先读评论对象缓存。运营端不限制 visible_status，但仍然只读取未删除评论。
	comment, hit, err := r.getCommentFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get comment cache failed, key=%s, err=%v", key, err)
	} else if hit {
		if comment == nil {
			return nil, gorm.ErrRecordNotFound
		}
		return comment, nil
	}

	// 缓存未命中时通过 singleflight 合并同 ID 回源，防止审核详情被并发打穿。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 复用其他请求刚写入的对象缓存。
		comment, hit, err := r.getCommentFromCache(ctx, key)
		if err == nil && hit {
			return comment, nil
		}

		// 回源 MySQL；不存在时写空值缓存，存在时回填对象缓存并补 Bloom。
		comment, err = r.queryCommentByIDFromDB(ctx, commentID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				_ = r.setCache(ctx, key, []byte(operatorNullCacheValue), operatorNullCacheTTL)
			}
			return nil, err
		}

		if data, err := json.Marshal(comment); err == nil {
			_ = r.setCache(ctx, key, data, operatorObjectCacheTTL)
		}

		_ = r.bloomAdd(ctx, operatorBloomCommentKey, commentID)

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
// 三、帖子读操作
// ============================================================

// GetPostByID 根据 post_id 查询帖子。
//
// 运营端可以查看所有状态的帖子，所以这里不限制 status=1。
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
func (r *operatorRepo) GetPostByID(ctx context.Context, postID int64) (*model.Post, error) {
	if postID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}

	// 运营端读取帖子也先过 Bloom Filter，减少无效 post_id 对后端存储的冲击。
	exists, err := r.bloomExists(ctx, operatorBloomPostKey, postID)
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
		operatorNullCacheValue,
		operatorObjectCacheTTL,
		operatorObjectCacheTTL,
		operatorNullCacheTTL,
		r.queryPostByIDFromDB,
		r.queryPostStatsByIDFromDB,
	)
	if err != nil {
		return nil, err
	}

	_ = r.bloomAdd(ctx, operatorBloomPostKey, postID)
	r.data.applyPendingPostLikeCountDelta(ctx, post)
	r.data.applyPendingPostCommentCountDelta(ctx, post)
	return post, nil
}

// ============================================================
// 五、聚合查询
// ============================================================

// GetPostDetailWithComments 查询帖子详情及评论列表。
//
// 复用已有缓存方法：
// 1. GetPostByID 负责帖子对象缓存；
// 2. ListPostCommentsForOperator 负责运营端评论列表 ID 缓存和评论对象 MGET；
// 3. 该方法本身不直接查 MySQL。
func (r *operatorRepo) GetPostDetailWithComments(
	ctx context.Context,
	postID int64,
	pageNum, pageSize int32,
) (*model.Post, []*model.StudyComment, int64, error) {
	post, err := r.GetPostByID(ctx, postID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, 0, errors.New("帖子不存在")
		}
		return nil, nil, 0, err
	}

	comments, total, err := r.ListPostCommentsForOperator(ctx, postID, pageNum, pageSize)
	if err != nil {
		return nil, nil, 0, err
	}

	return post, comments, total, nil
}

// ListPostCommentsForOperator 查询运营端某帖子下的评论列表。
//
// 查询链路和审核列表一致，列表 key 只缓存 comment_id 和 total，
// 具体评论内容统一从 mysql:comment:{comment_id} 对象缓存读取。
//
// 运营端可以看到该帖子下所有未删除评论，包括待审核、已通过、已驳回。
func (r *operatorRepo) ListPostCommentsForOperator(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	// 统一分页参数，避免同一页在缓存和 MySQL 中产生不同语义。
	pageNum, pageSize = normalizeOperatorPage(pageNum, pageSize)

	// 运营端帖子评论列表缓存按 post_id + page 参数拆分。
	// 与学生/助教端不同，这里不按 visible_status 过滤。
	key := buildOperatorPostCommentListCacheKey(postID, pageNum, pageSize)

	// 先读列表 ID 缓存，命中后再批量加载完整评论对象。
	cache, hit, err := r.getCommentListFromCache(ctx, key)
	if err != nil {
		r.log.WithContext(ctx).Warnf("get operator post comments cache failed, key=%s, err=%v", key, err)
	} else if hit {
		comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
		return comments, cache.Total, err
	}

	// 缓存未命中时合并同一帖子同一页的回源查询。
	val, err, _ := r.mysqlGroup.Do(key, func() (interface{}, error) {
		// double check 复用其他请求刚写入的列表缓存。
		cache, hit, err := r.getCommentListFromCache(ctx, key)
		if err == nil && hit {
			return cache, nil
		}

		// 回源查询该帖子下所有未删除评论，包含待审、通过和驳回。
		comments, total, err := r.queryPostCommentsForOperatorFromDB(ctx, postID, pageNum, pageSize)
		if err != nil {
			return nil, err
		}

		// 回填对象缓存后，列表缓存只保留 ID 和 total。
		r.setCommentObjectCaches(ctx, comments)

		// 重建短 TTL 列表缓存；评论新增/删除/审核会删除相关前缀缓存。
		cache = newOperatorCommentListCache(comments, total)

		if data, err := json.Marshal(cache); err == nil {
			_ = r.setCache(ctx, key, data, operatorListCacheTTL)
		}

		return cache, nil
	})
	if err != nil {
		return nil, 0, err
	}

	// 类型校验后按 comment_id 顺序装载对象，保持分页列表顺序稳定。
	cache, ok := val.(*operatorCommentListCache)
	if !ok || cache == nil {
		return nil, 0, fmt.Errorf("invalid operator post comment list cache result")
	}

	comments, err := r.loadCommentsByIDs(ctx, cache.CommentIDs, r.queryCommentsByIDsFromDB)
	return comments, cache.Total, err
}

// ============================================================
// 六、MySQL 原始查询方法
//
// 说明：
// 1. 这些方法只查 DB；
// 2. 不读写缓存；
// 3. 外层方法负责 Redis、Bloom、singleflight。
// ============================================================

// queryCommentsByAuditStatusFromDB 从 MySQL 分页查询指定审核状态的评论。
//
// 返回值：
// 1. 当前页评论列表；
// 2. 满足审核状态条件的总数；
// 3. 查询错误。
func (r *operatorRepo) queryCommentsByAuditStatusFromDB(ctx context.Context, auditStatus int32, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	c := r.data.q.StudyComment

	// offset 是 MySQL 分页起点。
	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.AuditStatus.Eq(auditStatus), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
}

// queryCommentByIDFromDB 从 MySQL 查询单条未删除评论。
//
// 运营端审核详情、审核动作、评论详情都会使用该强一致读取。
func (r *operatorRepo) queryCommentByIDFromDB(ctx context.Context, commentID int64) (*model.StudyComment, error) {
	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.Eq(commentID), c.DeletedAt.IsNull()).
		First()
}

// queryCommentsByIDsFromDB 按 comment_id 批量查询未删除评论。
//
// 入参通常来自列表 ID 缓存；返回值是仍然存在的评论对象。
func (r *operatorRepo) queryCommentsByIDsFromDB(ctx context.Context, commentIDs []int64) ([]*model.StudyComment, error) {
	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	c := r.data.q.StudyComment

	return c.WithContext(ctx).
		Where(c.CommentID.In(commentIDs...), c.DeletedAt.IsNull()).
		Find()
}

// queryPostByIDFromDB 从 MySQL 查询帖子详情。
//
// 运营端用于审核排障，可以查看任意状态帖子，因此这里不限制 status/deleted_at。
func (r *operatorRepo) queryPostByIDFromDB(ctx context.Context, postID int64) (*model.Post, error) {
	p := r.data.q.Post

	// 运营端可以查看所有状态帖子，这里不限制 status。
	post, err := p.WithContext(ctx).
		Where(p.PostID.Eq(postID)).
		First()
	if err != nil {
		return nil, err
	}
	if err := r.data.attachPersistedPostCounters(ctx, []*model.Post{post}); err != nil {
		return nil, err
	}
	return post, nil
}

func (r *operatorRepo) queryPostStatsByIDFromDB(ctx context.Context, postID int64) (*postStatsCache, error) {
	stats, err := r.data.queryPostCounterByID(ctx, postID)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// queryPostCommentsForOperatorFromDB 从 MySQL 查询帖子下所有未删除评论。
//
// 与学生/助教端不同，运营端需要看到待审、拒绝等不可见评论。
func (r *operatorRepo) queryPostCommentsForOperatorFromDB(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	c := r.data.q.StudyComment

	// offset 控制分页起点，按 created_at desc 让最新评论优先展示。
	offset := int((pageNum - 1) * pageSize)

	return c.WithContext(ctx).
		Where(c.PostID.Eq(postID), c.DeletedAt.IsNull()).
		Order(c.CreatedAt.Desc()).
		FindByPage(offset, int(pageSize))
}

// ============================================================
// 七、缓存结构体
// ============================================================

type operatorCommentListCache struct {
	Version    int     `json:"version"`
	CommentIDs []int64 `json:"comment_ids"`
	Total      int64   `json:"total"`
}

// newOperatorCommentListCache 把评论对象列表转换成运营端列表 ID 缓存。
//
// value 只保存 comment_id 列表和 total；后续读取时再通过对象缓存或 MySQL 拿完整评论。
func newOperatorCommentListCache(comments []*model.StudyComment, total int64) *operatorCommentListCache {
	return &operatorCommentListCache{
		Version:    operatorCommentListCacheVersion,
		CommentIDs: collectOperatorCommentIDs(comments),
		Total:      total,
	}
}

// collectOperatorCommentIDs 从评论对象中提取有效 comment_id。
//
// 返回顺序和原列表一致，用来保留 MySQL 查询排序。
func collectOperatorCommentIDs(comments []*model.StudyComment) []int64 {
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
// 八、对象缓存读取方法
// ============================================================

// getPostFromCache 读取运营端帖子对象缓存。
//
// 返回值：
// 1. post：缓存中的帖子对象；
// 2. hit：true 表示 Redis 命中，包含空值缓存命中；
// 3. error：Redis 或 JSON 解析错误。
func (r *operatorRepo) getPostFromCache(ctx context.Context, key string) (*model.Post, bool, error) {
	// getCache 返回 Redis 原始 bytes；len(data)==0 表示未命中。
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == operatorNullCacheValue {
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
func (r *operatorRepo) getCommentFromCache(ctx context.Context, key string) (*model.StudyComment, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == operatorNullCacheValue {
		// 空值缓存命中时直接返回，不再访问 MySQL。
		return nil, true, nil
	}

	var comment model.StudyComment
	if err := json.Unmarshal(data, &comment); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}

	return &comment, true, nil
}

// loadCommentsByIDs 按 comment_id 顺序批量加载评论对象。
//
// 调用链：
// 1. getCommentsFromObjectCache 先 MGET 评论对象缓存，返回 cached map 和 missed IDs；
// 2. queryMissed 只查询缓存未命中的评论；
// 3. setCommentObjectCaches 把回源评论写入对象缓存；
// 4. 最后按 commentIDs 原顺序组装结果返回。
func (r *operatorRepo) loadCommentsByIDs(ctx context.Context, commentIDs []int64, queryMissed func(context.Context, []int64) ([]*model.StudyComment, error)) ([]*model.StudyComment, error) {
	if len(commentIDs) == 0 {
		return []*model.StudyComment{}, nil
	}

	// cached 保存已命中的 comment_id -> comment，missed 是需要回源 MySQL 的 ID。
	cached, missed, err := r.getCommentsFromObjectCache(ctx, commentIDs)
	if err != nil {
		r.log.WithContext(ctx).Warnf("mget operator comment object cache failed, err=%v", err)
		missed = commentIDs
	}

	if len(missed) > 0 {
		// queryMissed 返回仍然未删除的评论对象，具体筛选规则由调用方决定。
		comments, err := queryMissed(ctx, missed)
		if err != nil {
			return nil, err
		}
		// 读路径回源后才写对象缓存；写路径只做缓存删除。
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
func (r *operatorRepo) getCommentsFromObjectCache(ctx context.Context, commentIDs []int64) (map[int64]*model.StudyComment, []int64, error) {
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
		keys = append(keys, buildOperatorCommentCacheKey(commentID))
	}

	// MGET 一次读取多个评论对象，span 会记录命中和未命中数量。
	cacheCtx, cacheSpan := observability.StartSpan(ctx, "redis.MGET mysql:comment",
		attribute.String("db.system", "redis"),
		attribute.String("db.operation", "MGET"),
		attribute.String("app.role", "operator"),
		attribute.String("cache.key.prefix", operatorCommentCachePrefix),
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
			// Redis nil 表示对象缓存未命中。
			missed = append(missed, commentID)
			continue
		}

		// redisValueBytes 兼容 go-redis 返回 string 或 []byte 两种类型。
		raw, ok := redisValueBytes(value)
		if !ok {
			missed = append(missed, commentID)
			continue
		}
		if string(raw) == operatorNullCacheValue {
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
// 只在读路径回源 MySQL 后调用；审核、新增、删除等写路径不会主动加载列表缓存。
func (r *operatorRepo) setCommentObjectCaches(ctx context.Context, comments []*model.StudyComment) {
	for _, comment := range comments {
		if comment == nil || comment.CommentID <= 0 {
			continue
		}
		if data, err := json.Marshal(comment); err == nil {
			_ = r.setCache(ctx, buildOperatorCommentCacheKey(comment.CommentID), data, operatorObjectCacheTTL)
		}
		_ = r.bloomAdd(ctx, operatorBloomCommentKey, comment.CommentID)
	}
}

// ============================================================
// 九、列表缓存读取方法
// ============================================================

// getCommentListFromCache 读取运营端评论列表 ID 缓存。
//
// 返回 operatorCommentListCache，其中 CommentIDs 是当前页评论 ID，Total 是总数；
// 外层还会调用 loadCommentsByIDs 获取完整评论对象。
func (r *operatorRepo) getCommentListFromCache(ctx context.Context, key string) (*operatorCommentListCache, bool, error) {
	data, err := r.getCache(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var cache operatorCommentListCache
	if err := json.Unmarshal(data, &cache); err != nil {
		_ = r.delCache(ctx, key)
		return nil, false, err
	}
	if cache.Version != operatorCommentListCacheVersion {
		// 版本不匹配说明缓存结构升级，删除旧缓存并让读路径重建。
		_ = r.delCache(ctx, key)
		return nil, false, nil
	}
	if cache.CommentIDs == nil {
		cache.CommentIDs = []int64{}
	}

	return &cache, true, nil
}

// ============================================================
// 十、Redis 基础操作
// ============================================================

// getCache 封装 Redis GET。
//
// 返回 nil,nil 表示缓存未命中、Redis 未配置或本次请求开启了 cache bypass。
func (r *operatorRepo) getCache(ctx context.Context, key string) ([]byte, error) {
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
// 返回 error 仅表示 Redis 写入失败，调用方可记录日志但不影响 MySQL 主流程。
func (r *operatorRepo) setCache(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if r.data.cache == nil || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Set(ctx, key, data, cacheTTLWithJitter(ttl)).Err()
}

// delCache 删除一个或多个 Redis key。
//
// 写路径调用该方法做缓存失效，删除失败会返回 error 给外层记录。
func (r *operatorRepo) delCache(ctx context.Context, keys ...string) error {
	if r.data.cache == nil || len(keys) == 0 {
		return nil
	}

	return r.data.cache.Del(ctx, keys...).Err()
}

// ============================================================
// 十一、Bloom Filter
// ============================================================

// bloomExists 查询 RedisBloom 中是否可能存在指定 ID。
//
// 返回 true 表示“可能存在”，false 表示“一定不存在”；RedisBloom 不可用时返回 true 放行。
func (r *operatorRepo) bloomExists(ctx context.Context, bloomKey string, id int64) (bool, error) {
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

// bloomAdd 将已确认存在的 ID 写入 Bloom Filter。
//
// Bloom 只用于减少无效 ID 回源 MySQL，不承担数据一致性职责。
func (r *operatorRepo) bloomAdd(ctx context.Context, bloomKey string, id int64) error {
	if r.data.cache == nil || id <= 0 || cachecontrol.Bypass(ctx) {
		return nil
	}

	return r.data.cache.Do(ctx, "BF.ADD", bloomKey, strconv.FormatInt(id, 10)).Err()
}

// ============================================================
// 十二、缓存 key 构造
// ============================================================

// buildOperatorPostCacheKey 生成运营端帖子对象缓存 key，格式为 mysql:post:{post_id}。
func buildOperatorPostCacheKey(postID int64) string {
	return operatorPostCachePrefix + strconv.FormatInt(postID, 10)
}

// buildOperatorCommentCacheKey 生成运营端评论对象缓存 key，格式为 mysql:comment:{comment_id}。
func buildOperatorCommentCacheKey(commentID int64) string {
	return operatorCommentCachePrefix + strconv.FormatInt(commentID, 10)
}

// buildOperatorAuditCommentListCacheKeyPrefix 生成某个审核状态的评论列表缓存前缀。
//
// 审核通过/拒绝后会按该前缀删除相关分页缓存。
func buildOperatorAuditCommentListCacheKeyPrefix(auditStatus int32) string {
	return operatorAuditCommentListCachePrefix + strconv.FormatInt(int64(auditStatus), 10) + ":"
}

// buildOperatorAuditCommentListCacheKey 生成审核列表缓存 key。
//
// value 保存 comment_id 列表和 total，page_num/page_size 通过 hash 参与区分。
func buildOperatorAuditCommentListCacheKey(auditStatus int32, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"audit_status": auditStatus,
		"page_num":     pageNum,
		"page_size":    pageSize,
	}

	return buildOperatorAuditCommentListCacheKeyPrefix(auditStatus) + hashOperatorCacheParam(param)
}

// buildOperatorPostCommentListCacheKeyPrefix 生成某个帖子的运营评论列表缓存前缀。
//
// 新增、删除、审核评论后会按该前缀删除不同分页参数下的列表缓存。
func buildOperatorPostCommentListCacheKeyPrefix(postID int64) string {
	return operatorPostCommentListCachePrefix + strconv.FormatInt(postID, 10) + ":"
}

// buildOperatorPostCommentListCacheKey 生成运营端帖子评论列表缓存 key。
//
// 该 key 对应一个分页窗口，value 是 comment_id 列表和 total。
func buildOperatorPostCommentListCacheKey(postID int64, pageNum, pageSize int32) string {
	param := map[string]interface{}{
		"post_id":   postID,
		"page_num":  pageNum,
		"page_size": pageSize,
	}

	return buildOperatorPostCommentListCacheKeyPrefix(postID) + hashOperatorCacheParam(param)
}

// hashOperatorCacheParam 将缓存参数序列化后计算 sha1，生成固定长度 key 后缀。
func hashOperatorCacheParam(v interface{}) string {
	data, _ := json.Marshal(v)
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}

// ============================================================
// 十三、分页工具
// ============================================================

// normalizeOperatorPage 统一修正运营端分页参数。
//
// 返回值 pageNum 至少为 1，pageSize 默认 5，最大 100，避免大分页拖慢 DB。
func normalizeOperatorPage(pageNum, pageSize int32) (int32, int32) {
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
