package service

import (
	"strconv"

	commentv1 "comment-operator/api/comment/v1"
	"comment-operator/internal/auth"
	"comment-operator/internal/client"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewOperatorCommentService)

type OperatorCommentService struct {
	comment *client.CommentClient
	log     *log.Helper
}

func NewOperatorCommentService(comment *client.CommentClient, logger log.Logger) *OperatorCommentService {
	return &OperatorCommentService{
		comment: comment,
		log:     log.NewHelper(log.With(logger, "module", "service/comment-operator")),
	}
}

type auditCommentRequest struct {
	Action             int32  `json:"action"`
	ManualReviewReason string `json:"manual_review_reason"`
}

func (s *OperatorCommentService) GetPostDetail(ctx http.Context) error {
	p, err := auth.RequireOperator(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Operator.GetPostDetailOperator(ctx, &commentv1.GetPostDetailRequest{
		PostId:          postID,
		UserId:          p.UserID,
		CommentPageNum:  queryInt32(ctx, "comment_page_num", 1),
		CommentPageSize: queryInt32(ctx, "comment_page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *OperatorCommentService) ListComments(ctx http.Context) error {
	p, err := auth.RequireOperator(ctx)
	if err != nil {
		return err
	}
	reply, err := s.comment.Operator.ListPendingComments(ctx, &commentv1.ListPendingCommentsRequest{
		OperatorId:  p.UserID,
		PageNum:     queryInt32(ctx, "page_num", 1),
		PageSize:    queryInt32(ctx, "page_size", 20),
		AuditStatus: int32(queryInt64(ctx, "audit_status", 0)),
	})
	return ctx.Returns(reply, err)
}

func (s *OperatorCommentService) GetCommentAuditDetail(ctx http.Context) error {
	p, err := auth.RequireOperator(ctx)
	if err != nil {
		return err
	}
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Operator.GetCommentAuditDetail(ctx, &commentv1.GetCommentAuditDetailRequest{
		OperatorId: p.UserID,
		CommentId:  commentID,
	})
	return ctx.Returns(reply, err)
}

func (s *OperatorCommentService) AuditComment(ctx http.Context) error {
	p, err := auth.RequireOperator(ctx)
	if err != nil {
		return err
	}
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	var req auditCommentRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if req.Action != 1 && req.Action != 2 {
		return kerrors.BadRequest("BAD_REQUEST", "action must be 1 or 2")
	}
	if req.Action == 2 && req.ManualReviewReason == "" {
		return kerrors.BadRequest("BAD_REQUEST", "manual_review_reason is required when rejecting")
	}
	reply, err := s.comment.Operator.AuditComment(ctx, &commentv1.AuditCommentRequest{
		OperatorId:         p.UserID,
		CommentId:          commentID,
		Action:             req.Action,
		ManualReviewReason: req.ManualReviewReason,
	})
	return ctx.Returns(reply, err)
}

func (s *OperatorCommentService) SearchComments(ctx http.Context) error {
	reply, err := s.comment.Search.SearchOperatorComments(ctx, &commentv1.SearchOperatorCommentsRequest{
		AuditStatus:      int32(queryInt64(ctx, "audit_status", -1)),
		Keyword:          ctx.Query().Get("keyword"),
		ManualOperatorId: queryInt64(ctx, "manual_operator_id", 0),
		ReviewReason:     ctx.Query().Get("review_reason"),
		StartTime:        queryInt64(ctx, "start_time", 0),
		EndTime:          queryInt64(ctx, "end_time", 0),
		PageNum:          queryInt32(ctx, "page_num", 1),
		PageSize:         queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func pathInt64(ctx http.Context, key string) (int64, error) {
	raw := ctx.Vars().Get(key)
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return 0, kerrors.BadRequest("BAD_REQUEST", key+" must be a positive integer")
	}
	return value, nil
}

func queryInt64(ctx http.Context, key string, def int64) int64 {
	raw := ctx.Query().Get(key)
	if raw == "" {
		return def
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return def
	}
	return value
}

func queryInt32(ctx http.Context, key string, def int32) int32 {
	raw := ctx.Query().Get(key)
	if raw == "" {
		return def
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value <= 0 {
		return def
	}
	if value > 100 {
		return 100
	}
	return int32(value)
}
