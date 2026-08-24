package service

import (
	"strconv"

	commentv1 "comment-student/api/comment/v1"
	"comment-student/internal/auth"
	"comment-student/internal/client"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewStudentCommentService)

type StudentCommentService struct {
	comment *client.CommentClient
	log     *log.Helper
}

func NewStudentCommentService(comment *client.CommentClient, logger log.Logger) *StudentCommentService {
	return &StudentCommentService{
		comment: comment,
		log:     log.NewHelper(log.With(logger, "module", "service/comment-student")),
	}
}

type createCommentRequest struct {
	Content string `json:"content"`
}

func (s *StudentCommentService) GetPostDetail(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	pageNum := queryInt32(ctx, "comment_page_num", 1)
	pageSize := queryInt32(ctx, "comment_page_size", 20)

	reply, err := s.comment.Student.GetPostDetailStudent(ctx, &commentv1.GetPostDetailRequest{
		PostId:          postID,
		UserId:          p.UserID,
		CommentPageNum:  pageNum,
		CommentPageSize: pageSize,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) ListPostComments(ctx http.Context) error {
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.ListPostCommentsStudent(ctx, &commentv1.ListPostCommentsRequest{
		PostId:   postID,
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) CreateComment(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	var req createCommentRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if req.Content == "" {
		return kerrors.BadRequest("BAD_REQUEST", "content is required")
	}

	reply, err := s.comment.Student.CreateComment(ctx, &commentv1.CreateCommentRequest{
		StudentId: p.UserID,
		PostId:    postID,
		Content:   req.Content,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) DeleteMyComment(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.DeleteMyComment(ctx, &commentv1.DeleteMyCommentRequest{
		StudentId: p.UserID,
		CommentId: commentID,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) GetCommentDetail(ctx http.Context) error {
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.GetCommentDetailStudent(ctx, &commentv1.GetCommentDetailRequest{
		CommentId: commentID,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) ListMyComments(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.ListMyComments(ctx, &commentv1.ListMyCommentsRequest{
		StudentId: p.UserID,
		PageNum:   queryInt32(ctx, "page_num", 1),
		PageSize:  queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) LikePost(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.LikePost(ctx, &commentv1.LikePostRequest{
		StudentId: p.UserID,
		PostId:    postID,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) UnlikePost(ctx http.Context) error {
	p, err := auth.RequireStudent(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Student.UnlikePost(ctx, &commentv1.UnlikePostRequest{
		StudentId: p.UserID,
		PostId:    postID,
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) SearchPosts(ctx http.Context) error {
	reply, err := s.comment.Search.SearchStudentPosts(ctx, &commentv1.SearchStudentPostsRequest{
		Keyword:  ctx.Query().Get("keyword"),
		TutorId:  queryInt64(ctx, "tutor_id", 0),
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *StudentCommentService) SearchComments(ctx http.Context) error {
	reply, err := s.comment.Search.SearchStudentComments(ctx, &commentv1.SearchStudentCommentsRequest{
		Keyword:  ctx.Query().Get("keyword"),
		PostId:   queryInt64(ctx, "post_id", 0),
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
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
