package service

import (
	"context"

	pb "comment-service/api/comment/v1"
	"comment-service/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

// OperatorService 承接运营端 proto 服务入口。
//
// 运营端主要处理审核列表、审核详情、审核动作和运营视角帖子详情。
type OperatorService struct {
	pb.UnimplementedOperatorServiceServer
	uc  *biz.OperatorUsecase
	log *log.Helper
}

// NewOperatorService 创建运营端 RPC/HTTP 服务处理器。
func NewOperatorService(uc *biz.OperatorUsecase, logger log.Logger) *OperatorService {
	return &OperatorService{
		uc:  uc,
		log: log.NewHelper(log.With(logger, "module", "service/operator")),
	}
}

// ListPendingComments 查询运营审核列表。
//
// audit_status 可表示待审核、已通过、已驳回；返回当前页评论 DTO 和 total。
func (s *OperatorService) ListPendingComments(ctx context.Context, req *pb.ListPendingCommentsRequest) (*pb.ListPendingCommentsReply, error) {
	// Biz 层规范化分页参数，data 层按 audit_status 读取审核列表缓存或 MySQL。
	comments, total, err := s.uc.ListPendingComments(ctx, req.AuditStatus, req.PageNum, req.PageSize)
	if err != nil {
		return nil, err
	}

	return &pb.ListPendingCommentsReply{
		Items: commentDTOsWithoutReplies(comments),
		Total: total,
	}, nil
}

// GetCommentAuditDetail 查询单条评论的审核详情。
//
// 返回评论本体和所属帖子上下文，便于运营判断评论是否违规。
func (s *OperatorService) GetCommentAuditDetail(ctx context.Context, req *pb.GetCommentAuditDetailRequest) (*pb.GetCommentAuditDetailReply, error) {
	// Biz 层聚合 comment 和 post；service 负责组装审核详情 DTO。
	comment, post, err := s.uc.GetCommentAuditDetail(ctx, req.CommentId)
	if err != nil {
		return nil, err
	}

	reply := &pb.GetCommentAuditDetailReply{
		Comment: commentDTOWithoutReplies(comment),
	}

	// 组装帖子上下文以辅助运营审核
	if post != nil {
		reply.Post = &pb.PostDTO{
			PostId:   post.PostID,
			AuthorId: post.AuthorID,
			Title:    post.Title,
			Content:  post.Content, // 审核时可能需要看帖子的具体内容对比
			Status:   post.Status,
		}
	}

	return reply, nil
}

// AuditComment 执行运营审核动作。
//
// action=1 表示通过，action=2 表示驳回；返回空 reply 表示状态变更成功。
func (s *OperatorService) AuditComment(ctx context.Context, req *pb.AuditCommentRequest) (*pb.AuditCommentReply, error) {
	// Biz 层会校验审核动作、评论当前状态，并调用 data 层做条件更新。
	err := s.uc.AuditComment(ctx, req.OperatorId, req.CommentId, req.Action, req.ManualReviewReason)
	if err != nil {
		return nil, err
	}

	return &pb.AuditCommentReply{}, nil
}

// GetPostDetailOperator 查询运营端帖子详情。
//
// 运营端可以查看帖子下所有未删除评论，包括待审核、通过、驳回，用于排查和回溯。
func (s *OperatorService) GetPostDetailOperator(ctx context.Context, req *pb.GetPostDetailRequest) (*pb.GetPostDetailReply, error) {
	// 设置默认分页参数
	if req.CommentPageNum <= 0 {
		req.CommentPageNum = 1
	}
	if req.CommentPageSize <= 0 || req.CommentPageSize > 100 {
		req.CommentPageSize = 20
	}

	// Biz 返回帖子、运营端评论页和 total；data 层不会限制 status=1。
	post, comments, totalComments, err := s.uc.GetPostDetailOperator(ctx, req.PostId, req.CommentPageNum, req.CommentPageSize)
	if err != nil {
		return nil, err
	}

	// 组装帖子信息
	postDTO := postDTOFromModel(post)

	return &pb.GetPostDetailReply{
		Post:          postDTO,
		Comments:      commentDTOsFromModels(comments, nil),
		TotalComments: totalComments,
	}, nil
}
