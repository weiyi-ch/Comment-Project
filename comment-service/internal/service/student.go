package service

import (
	"context"
	"errors"

	pb "comment-service/api/comment/v1"
	"comment-service/dal/model"
	"comment-service/internal/biz"
	"comment-service/internal/observability"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"go.opentelemetry.io/otel/attribute"
)

// StudentService 承接学生端 proto 服务入口。
//
// service 层只负责请求 DTO 与业务模型的转换，权限、状态机和缓存细节都交给 biz/data 层。
type StudentService struct {
	pb.UnimplementedStudentServiceServer
	uc  *biz.StudentUsecase
	log *log.Helper
}

// NewStudentService 创建学生端 RPC/HTTP 服务处理器。
func NewStudentService(uc *biz.StudentUsecase, logger log.Logger) *StudentService {
	return &StudentService{
		uc:  uc,
		log: log.NewHelper(log.With(logger, "module", "service/student")),
	}
}

// LikePost 处理学生点赞帖子请求。
//
// 入参来自 proto 的 student_id/post_id；返回空 reply 表示操作成功。
// 具体的幂等、短锁、Redis 计数入队都在 StudentUsecase 和 data 层完成。
func (s *StudentService) LikePost(ctx context.Context, req *pb.LikePostRequest) (*pb.LikePostReply, error) {
	// 调用 Biz 层执行点赞业务；返回值只有 error，成功时无需额外数据。
	err := s.uc.LikePost(ctx, req.StudentId, req.PostId)
	if err != nil {
		// 同一学生同一帖子短时间重复操作属于业务限流，压测中应表现为 429 而不是 500。
		if errors.Is(err, biz.ErrPostLikeBusy) {
			return nil, kerrors.New(429, "POST_LIKE_BUSY", err.Error())
		}
		return nil, err
	}
	return &pb.LikePostReply{}, nil
}

// UnlikePost 处理学生取消点赞请求。
//
// service 层只做参数透传，并把重复取消/锁忙映射成可预期的业务错误。
func (s *StudentService) UnlikePost(ctx context.Context, req *pb.UnlikePostRequest) (*pb.UnlikePostReply, error) {
	// 调用 Biz 层取消点赞；成功返回空 reply，失败直接把业务错误交给 Kratos。
	err := s.uc.UnlikePost(ctx, req.StudentId, req.PostId)
	if err != nil {
		// 未点赞或已经取消过时，返回 409 表示当前状态不满足取消条件，不再扣减计数。
		if errors.Is(err, biz.ErrPostNotLiked) {
			return nil, kerrors.Conflict("POST_NOT_LIKED", err.Error())
		}
		// 和点赞一样，取消点赞短锁竞争属于可预期业务限流。
		if errors.Is(err, biz.ErrPostLikeBusy) {
			return nil, kerrors.New(429, "POST_LIKE_BUSY", err.Error())
		}
		return nil, err
	}
	return &pb.UnlikePostReply{}, nil
}

// CreateComment 处理学生发表评论请求。
//
// 返回值包含新生成的 comment_id 和 audit_status，方便前端知道评论已创建以及当前审核状态。
func (s *StudentService) CreateComment(ctx context.Context, req *pb.CreateCommentRequest) (*pb.CreateCommentReply, error) {
	// 把 proto 请求转换成数据库模型；comment_id、审核状态由 Biz 层统一初始化。
	commentModel := &model.StudyComment{
		PostID:    req.PostId,
		StudentID: req.StudentId,
		Content:   req.Content,
	}

	// 调用 Biz 层创建评论；返回完整评论模型，里面带新生成的 CommentID/AuditStatus。
	result, err := s.uc.CreateComment(ctx, commentModel)
	if err != nil {
		return nil, err
	}

	return &pb.CreateCommentReply{
		CommentId:   result.CommentID,
		AuditStatus: result.AuditStatus,
	}, nil
}

// DeleteMyComment 处理学生删除自己评论的请求。
//
// 返回空 reply 表示删除成功；是否有权限删除由 Biz 层根据 comment.student_id 校验。
func (s *StudentService) DeleteMyComment(ctx context.Context, req *pb.DeleteMyCommentRequest) (*pb.DeleteMyCommentReply, error) {
	// Biz 层会先查评论归属，再调用 data 层软删除并扣减评论数。
	err := s.uc.DeleteMyComment(ctx, req.StudentId, req.CommentId)
	if err != nil {
		return nil, err
	}
	return &pb.DeleteMyCommentReply{}, nil
}

// ListPostCommentsStudent 查询学生端某个帖子的评论分页。
//
// 返回当前页 CommentDTO 和 total；data 层会处理列表 ID 缓存、评论对象 MGET 和 MySQL 回源。
func (s *StudentService) ListPostCommentsStudent(ctx context.Context, req *pb.ListPostCommentsRequest) (*pb.ListPostCommentsReply, error) {
	// Biz 返回数据库模型列表和总数；service 负责把模型转成 proto DTO。
	comments, total, err := s.uc.ListPostCommentsStudent(ctx, req.PostId, req.PageNum, req.PageSize)
	if err != nil {
		return nil, err
	}

	return &pb.ListPostCommentsReply{
		Items: commentDTOsWithoutReplies(comments),
		Total: total,
	}, nil
}

// GetCommentDetailStudent 查询评论详情。
//
// 返回评论主体、该评论下的助教回复，以及所属帖子摘要。
func (s *StudentService) GetCommentDetailStudent(ctx context.Context, req *pb.GetCommentDetailRequest) (*pb.GetCommentDetailReply, error) {
	// Biz 层聚合 comment、replies、post；service 只负责 DTO 组装。
	comment, replies, post, err := s.uc.GetCommentDetailStudent(ctx, req.CommentId)
	if err != nil {
		return nil, err
	}

	reply := &pb.GetCommentDetailReply{
		Comment: commentDTOFromModel(comment, replies),
	}

	// 组装所属帖子摘要信息
	if post != nil {
		reply.PostInfo = &pb.PostDTO{
			PostId:   post.PostID,
			AuthorId: post.AuthorID,
			Title:    post.Title,
		}
	}

	return reply, nil
}

// ListMyComments 查询学生自己的评论分页。
//
// 返回值是学生维度的评论列表和 total，用于“我的评论”页面。
func (s *StudentService) ListMyComments(ctx context.Context, req *pb.ListMyCommentsRequest) (*pb.ListMyCommentsReply, error) {
	// Biz/data 会按 student_id 做分页查询，并通过我的评论列表缓存加速。
	comments, total, err := s.uc.ListMyComments(ctx, req.StudentId, req.PageNum, req.PageSize)
	if err != nil {
		return nil, err
	}

	return &pb.ListMyCommentsReply{
		Items: commentDTOsWithoutReplies(comments),
		Total: total,
	}, nil
}

// GetPostDetailStudent 查询学生端帖子详情。
//
// 返回帖子主体、当前页评论、每条评论的回复，以及 total_comments。
// 这是学生端最核心的聚合读接口，也是压测和链路追踪重点观察的入口。
func (s *StudentService) GetPostDetailStudent(ctx context.Context, req *pb.GetPostDetailRequest) (reply *pb.GetPostDetailReply, err error) {
	// 创建 service 层入口 span；返回的新 ctx 会把 trace 上下文传给 usecase/data 子 span。
	ctx, span := observability.StartSpan(ctx, "StudentService.GetPostDetailStudent",
		attribute.Int64("post_id", req.PostId),
		attribute.Int64("user_id", req.UserId),
		attribute.Int("comment_page_num", int(req.CommentPageNum)),
		attribute.Int("comment_page_size", int(req.CommentPageSize)),
	)
	defer func() { observability.EndSpan(span, err) }()

	// 设置默认分页参数
	if req.CommentPageNum <= 0 {
		req.CommentPageNum = 1
	}
	if req.CommentPageSize <= 0 || req.CommentPageSize > 100 {
		req.CommentPageSize = 20
	}

	// Biz 层返回帖子、当前页评论和评论总数；回复另行批量查询，避免隐藏 N+1。
	post, comments, totalComments, err := s.uc.GetPostDetailStudent(ctx, req.PostId, req.CommentPageNum, req.CommentPageSize)
	if err != nil {
		return nil, err
	}

	// 组装帖子信息
	postDTO := postDTOFromModel(post)

	span.SetAttributes(
		attribute.Int("comments.count", len(comments)),
		attribute.Int64("total_comments", totalComments),
	)

	return &pb.GetPostDetailReply{
		Post:          postDTO,
		Comments:      commentDTOsFromModels(comments, nil),
		TotalComments: totalComments,
	}, nil
}
