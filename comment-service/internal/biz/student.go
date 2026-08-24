package biz

import (
	"context"
	"errors"

	"comment-service/dal/model"
	"comment-service/internal/observability"
	"comment-service/pkg/snowflake"

	"github.com/go-kratos/kratos/v2/log"
	"go.opentelemetry.io/otel/attribute"
)

var (
	// ErrPostLikeBusy 表示同一学生同一帖子的点赞/取消点赞请求过于密集。
	ErrPostLikeBusy = errors.New("点赞操作过于频繁，请稍后重试")
	// ErrPostNotLiked 表示取消点赞时当前没有有效点赞关系。
	ErrPostNotLiked = errors.New("该帖子尚未点赞，请勿重复取消")
)

// StudentRepo 定义学生端业务用例依赖的数据访问能力。
//
// biz 层通过接口依赖 data 层，便于把权限校验、状态初始化和存储实现解耦。
type StudentRepo interface {
	// 互动操作：同步写点赞关系，计数通过 Redis delta 异步落库。
	LikePost(ctx context.Context, studentID, postID int64) error
	UnlikePost(ctx context.Context, studentID, postID int64) error

	// 评论操作
	CreateComment(ctx context.Context, comment *model.StudyComment) (*model.StudyComment, error)
	GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error)
	DeleteComment(ctx context.Context, commentID int64) error

	// 查询操作
	ListPostCommentsStudent(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error)
	ListMyComments(ctx context.Context, studentID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error)

	// 聚合查询依赖
	GetPostByID(ctx context.Context, postID int64) (*model.Post, error)

	// 帖子详情查询（带分页评论）
	GetPostDetailWithComments(ctx context.Context, postID int64, pageNum, pageSize int32) (*model.Post, []*model.StudyComment, int64, error)
}

// StudentUsecase 封装学生端业务规则。
//
// 这里负责评论归属校验、默认分页、ID/状态初始化和聚合读编排。
type StudentUsecase struct {
	repo StudentRepo
	log  *log.Helper
}

// NewStudentUsecase 创建学生端业务用例。
func NewStudentUsecase(repo StudentRepo, logger log.Logger) *StudentUsecase {
	return &StudentUsecase{
		repo: repo,
		log:  log.NewHelper(log.With(logger, "module", "usecase/student")),
	}
}

// LikePost 执行学生点赞知识帖的业务入口。
//
// 返回值只有 error；点赞幂等、短锁、计数更新由 data 层完成。
func (uc *StudentUsecase) LikePost(ctx context.Context, studentID, postID int64) error {
	// repo.LikePost 返回 nil 表示点赞成功或幂等处理成功，返回 error 表示业务或存储失败。
	return uc.repo.LikePost(ctx, studentID, postID)
}

// UnlikePost 执行学生取消点赞知识帖的业务入口。
//
// 返回值只有 error；data 层会根据点赞旧状态决定是否扣减 like_count，重复取消会返回业务错误。
func (uc *StudentUsecase) UnlikePost(ctx context.Context, studentID, postID int64) error {
	// repo.UnlikePost 会处理“未点赞/重复取消”等状态机语义。
	return uc.repo.UnlikePost(ctx, studentID, postID)
}

// CreateComment 初始化评论 ID 和状态后写入评论。
//
// 返回创建后的评论模型，包含新生成的 comment_id 和 audit_status。
func (uc *StudentUsecase) CreateComment(ctx context.Context, comment *model.StudyComment) (*model.StudyComment, error) {
	// 1. 生成全局唯一ID
	comment.CommentID = snowflake.GenID()
	// 2. 状态初始化 (1可见, 0待机审/人工审)
	comment.VisibleStatus = 1
	comment.AuditStatus = 0

	// repo.CreateComment 会开启事务插入评论并维护 post.comment_count。
	return uc.repo.CreateComment(ctx, comment)
}

// DeleteMyComment 删除学生自己的评论，并在删除前校验评论归属权。
//
// 返回 nil 表示删除成功；如果评论不存在或 student_id 不匹配，会返回业务错误。
func (uc *StudentUsecase) DeleteMyComment(ctx context.Context, studentID, commentID int64) error {
	// 先查询评论主体，返回值用于做水平越权校验。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return errors.New("评论不存在")
	}
	if comment.StudentID != studentID {
		return errors.New("无权删除他人的评论")
	}

	// 校验通过后交给 data 层软删除评论并同步扣减帖子评论数。
	return uc.repo.DeleteComment(ctx, commentID)
}

// ListPostCommentsStudent 查询学生端可见的帖子评论列表。
//
// 返回当前页评论列表、total 和错误。data 层负责缓存和 MySQL 查询。
func (uc *StudentUsecase) ListPostCommentsStudent(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	// repo 会按学生端可见规则查询 visible_status=1 的评论。
	return uc.repo.ListPostCommentsStudent(ctx, postID, pageNum, pageSize)
}

// GetCommentDetailStudent 查询学生端评论详情，并聚合回复和帖子摘要。
//
// 返回 comment、replies、post 三类数据；如果评论不存在，返回业务错误。
func (uc *StudentUsecase) GetCommentDetailStudent(ctx context.Context, commentID int64) (*model.StudyComment, []*model.StudyCommentReply, *model.Post, error) {
	// 先查评论主体；返回的 post_id 会用于继续查询帖子摘要。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return nil, nil, nil, errors.New("评论不存在")
	}

	// 回复已经内嵌在评论行中，不再额外查询回复表。
	replies := replyModelsFromComment(comment)
	post, _ := uc.repo.GetPostByID(ctx, comment.PostID)

	return comment, replies, post, nil
}

// ListMyComments 查询学生自己的评论列表。
//
// 返回当前学生的评论分页结果和 total。
func (uc *StudentUsecase) ListMyComments(ctx context.Context, studentID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	// repo 按 student_id 查询评论，data 层处理我的评论列表 ID 缓存。
	return uc.repo.ListMyComments(ctx, studentID, pageNum, pageSize)
}

// GetPostDetailStudent 查询学生端帖子详情及当前页评论。
//
// 返回 post、当前页 comments、total_comments 和错误。
func (uc *StudentUsecase) GetPostDetailStudent(ctx context.Context, postID int64, pageNum, pageSize int32) (post *model.Post, comments []*model.StudyComment, total int64, err error) {
	// 创建 usecase 层 span，方便在 Jaeger 中区分 service 耗时和业务聚合耗时。
	ctx, span := observability.StartSpan(ctx, "StudentUsecase.GetPostDetailStudent",
		attribute.Int64("post_id", postID),
		attribute.Int("page_num", int(pageNum)),
		attribute.Int("page_size", int(pageSize)),
	)
	defer func() { observability.EndSpan(span, err) }()

	// 参数校验和默认值设置
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	// repo 返回帖子、当前页评论和评论总数；data 层负责缓存、singleflight 和 SQL。
	post, comments, total, err = uc.repo.GetPostDetailWithComments(ctx, postID, pageNum, pageSize)
	span.SetAttributes(
		attribute.Int("comments.count", len(comments)),
		attribute.Int64("total_comments", total),
	)
	return post, comments, total, err
}
