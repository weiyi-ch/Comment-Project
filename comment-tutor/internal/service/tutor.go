package service

import (
	"strconv"

	commentv1 "comment-tutor/api/comment/v1"
	"comment-tutor/internal/auth"
	"comment-tutor/internal/client"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewTutorCommentService)

type TutorCommentService struct {
	comment *client.CommentClient
	log     *log.Helper
}

func NewTutorCommentService(comment *client.CommentClient, logger log.Logger) *TutorCommentService {
	return &TutorCommentService{
		comment: comment,
		log:     log.NewHelper(log.With(logger, "module", "service/comment-tutor")),
	}
}

type postRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type replyCommentRequest struct {
	Content string `json:"content"`
}

func (s *TutorCommentService) CreatePost(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	var req postRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if req.Title == "" || req.Content == "" {
		return kerrors.BadRequest("BAD_REQUEST", "title and content are required")
	}
	reply, err := s.comment.Tutor.CreatePost(ctx, &commentv1.CreatePostRequest{
		TutorId: p.UserID,
		Title:   req.Title,
		Content: req.Content,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) UpdatePost(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	var req postRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if req.Title == "" || req.Content == "" {
		return kerrors.BadRequest("BAD_REQUEST", "title and content are required")
	}
	reply, err := s.comment.Tutor.UpdatePost(ctx, &commentv1.UpdatePostRequest{
		TutorId: p.UserID,
		PostId:  postID,
		Title:   req.Title,
		Content: req.Content,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) DeletePost(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.DeletePost(ctx, &commentv1.DeletePostRequest{
		TutorId: p.UserID,
		PostId:  postID,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) ListMyPosts(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.ListTutorPosts(ctx, &commentv1.ListTutorPostsRequest{
		TutorId:  p.UserID,
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
		Status:   int32(queryInt64(ctx, "status", 0)),
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) GetPostDetail(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.GetPostDetailTutor(ctx, &commentv1.GetPostDetailRequest{
		PostId:          postID,
		UserId:          p.UserID,
		CommentPageNum:  queryInt32(ctx, "comment_page_num", 1),
		CommentPageSize: queryInt32(ctx, "comment_page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) ListPostComments(ctx http.Context) error {
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.ListPostCommentsTutor(ctx, &commentv1.ListPostCommentsRequest{
		PostId:   postID,
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) GetCommentDetail(ctx http.Context) error {
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.GetCommentDetailTutor(ctx, &commentv1.GetCommentDetailRequest{
		CommentId: commentID,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) DeleteComment(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.DeleteCommentTutor(ctx, &commentv1.DeleteCommentTutorRequest{
		TutorId:   p.UserID,
		CommentId: commentID,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) ReplyComment(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	commentID, err := pathInt64(ctx, "comment_id")
	if err != nil {
		return err
	}
	var req replyCommentRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if req.Content == "" {
		return kerrors.BadRequest("BAD_REQUEST", "content is required")
	}
	reply, err := s.comment.Tutor.ReplyComment(ctx, &commentv1.ReplyCommentRequest{
		TutorId:   p.UserID,
		CommentId: commentID,
		Content:   req.Content,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) DeleteReply(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	replyID, err := pathInt64(ctx, "reply_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Tutor.DeleteReply(ctx, &commentv1.DeleteReplyRequest{
		TutorId:        p.UserID,
		CommentReplyId: replyID,
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) SearchPosts(ctx http.Context) error {
	p, err := auth.RequireTutor(ctx)
	if err != nil {
		return err
	}
	reply, err := s.comment.Search.SearchTutorPosts(ctx, &commentv1.SearchTutorPostsRequest{
		Keyword:  ctx.Query().Get("keyword"),
		TutorId:  p.UserID,
		PageNum:  queryInt32(ctx, "page_num", 1),
		PageSize: queryInt32(ctx, "page_size", 20),
	})
	return ctx.Returns(reply, err)
}

func (s *TutorCommentService) SearchComments(ctx http.Context) error {
	postID, err := pathInt64(ctx, "post_id")
	if err != nil {
		return err
	}
	reply, err := s.comment.Search.SearchTutorComments(ctx, &commentv1.SearchTutorCommentsRequest{
		PostId:   postID,
		Keyword:  ctx.Query().Get("keyword"),
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
