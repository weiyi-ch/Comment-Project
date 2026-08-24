package biz

import (
	"context"
	"errors"

	"comment-service/dal/model"

	"github.com/go-kratos/kratos/v2/log"
)

// OperatorRepo 定义运营端业务用例依赖的数据访问能力。
//
// 审核通过/驳回由 data 层做条件更新和事务兜底，biz 层只编排规则和状态校验。
type OperatorRepo interface {
	ListCommentsByAuditStatus(ctx context.Context, auditStatus int32, pageNum, pageSize int32) ([]*model.StudyComment, int64, error)
	GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error)
	GetPostByID(ctx context.Context, postID int64) (*model.Post, error)

	// 审核通过
	ApproveComment(ctx context.Context, commentID, operatorID int64) error
	// 审核驳回 (涉及连带隐藏和扣减评论数，由 Data 层开事务保证原子性)
	RejectComment(ctx context.Context, commentID, operatorID int64, reason string) error

	// 帖子详情查询（带分页评论）
	GetPostDetailWithComments(ctx context.Context, postID int64, pageNum, pageSize int32) (*model.Post, []*model.StudyComment, int64, error)
}

// OperatorUsecase 封装运营端审核业务规则。
//
// 这里负责审核动作校验、重复审核拦截和运营端聚合读编排。
type OperatorUsecase struct {
	repo OperatorRepo
	log  *log.Helper
}

// NewOperatorUsecase 创建运营端业务用例。
func NewOperatorUsecase(repo OperatorRepo, logger log.Logger) *OperatorUsecase {
	return &OperatorUsecase{
		repo: repo,
		log:  log.NewHelper(log.With(logger, "module", "usecase/operator")),
	}
}

// ListPendingComments 查询不同审核状态的评论列表。
//
// 返回当前页评论、total 和错误；auditStatus=0/1/2 分别表示待审/通过/驳回。
func (uc *OperatorUsecase) ListPendingComments(ctx context.Context, auditStatus int32, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	// repo 根据 auditStatus 查询审核列表缓存或 MySQL，并返回当前页和总数。
	return uc.repo.ListCommentsByAuditStatus(ctx, auditStatus, pageNum, pageSize)
}

// GetCommentAuditDetail 获取评论审核详情和帖子上下文。
//
// 返回 comment 和 post；评论不存在时返回业务错误。
func (uc *OperatorUsecase) GetCommentAuditDetail(ctx context.Context, commentID int64) (*model.StudyComment, *model.Post, error) {
	// 先查评论主体，返回的 post_id 用于补充帖子上下文。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return nil, nil, errors.New("评论不存在")
	}

	// 帖子上下文只辅助审核展示，查不到时返回 nil post。
	post, _ := uc.repo.GetPostByID(ctx, comment.PostID)

	return comment, post, nil
}

// AuditComment 执行运营审核动作。
//
// action=1 表示通过，action=2 表示驳回；返回 nil 表示状态更新成功。
func (uc *OperatorUsecase) AuditComment(ctx context.Context, operatorID, commentID int64, action int32, reason string) error {
	// 1. 校验动作合法性
	if action != 1 && action != 2 {
		return errors.New("无效的审核动作")
	}

	// 2. 校验评论是否存在且是否处于可审核状态
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return errors.New("评论不存在")
	}
	if comment.AuditStatus != 0 {
		return errors.New("该评论已被审核，请勿重复操作")
	}

	// 根据 action 分派到 data 层的条件更新方法；返回值是审核更新结果。
	if action == 1 {
		// 通过
		return uc.repo.ApproveComment(ctx, commentID, operatorID)
	} else {
		// 驳回
		if reason == "" {
			return errors.New("驳回必须填写原因")
		}
		return uc.repo.RejectComment(ctx, commentID, operatorID, reason)
	}
}

// GetPostDetailOperator 查看运营端帖子详情。
//
// 返回帖子、当前页评论和 total；运营端可以查看所有未删除评论。
func (uc *OperatorUsecase) GetPostDetailOperator(ctx context.Context, postID int64, pageNum, pageSize int32) (*model.Post, []*model.StudyComment, int64, error) {
	// 参数校验和默认值设置
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	// repo 聚合帖子和运营端评论页；回复由 service 层批量加载。
	return uc.repo.GetPostDetailWithComments(ctx, postID, pageNum, pageSize)
}

// GetCommentDetailOperator 查询运营端评论详情。
//
// 返回评论主体、回复列表和帖子上下文；主要用于排查单条评论。
func (uc *OperatorUsecase) GetCommentDetailOperator(ctx context.Context, commentID int64) (*model.StudyComment, []*model.StudyCommentReply, *model.Post, error) {
	// 先查评论主体，返回的 post_id 用于继续查询帖子。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return nil, nil, nil, errors.New("评论不存在")
	}

	// 一次性助教回复已经内嵌在评论行中。
	replies := replyModelsFromComment(comment)
	post, _ := uc.repo.GetPostByID(ctx, comment.PostID)

	return comment, replies, post, nil
}
