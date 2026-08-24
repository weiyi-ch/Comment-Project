package service

import (
	"context"
	"errors"

	pb "comment-service/api/comment/v1"
	"comment-service/dal/model"
	"comment-service/internal/biz"
	"comment-service/pkg/CopyCommonFields"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
)

// TutorService 承接助教端 proto 服务入口。
//
// 它负责把 HTTP/gRPC 请求转换为业务模型，并把 biz 层结果组装成 proto reply。
type TutorService struct {
	pb.UnimplementedTutorServiceServer
	uc  *biz.TutorUsecase
	log *log.Helper
}

// NewTutorService 创建助教端 RPC/HTTP 服务处理器。
func NewTutorService(uc *biz.TutorUsecase, logger log.Logger) *TutorService {
	return &TutorService{
		uc:  uc,
		log: log.NewHelper(log.With(logger, "module", "service/tutor")),
	}
}

// CreatePost 处理助教发布知识帖请求。
//
// 返回新生成的 post_id；帖子 ID、状态初始化由 TutorUsecase 完成。
func (s *TutorService) CreatePost(ctx context.Context, req *pb.CreatePostRequest) (*pb.CreatePostReply, error) {
	postModel := &model.Post{}

	// 使用反射工具拷贝同名字段 (Title, Content 等)
	cf.CopyCommonFields(postModel, req)
	// 手动补齐名称不一致的字段 (pb是TutorId, gen是AuthorID)
	postModel.AuthorID = req.TutorId

	// Biz 层会生成 post_id、设置默认状态，并调用 data 层写入 MySQL。
	result, err := s.uc.CreatePost(ctx, postModel)
	if err != nil {
		return nil, err
	}
	return &pb.CreatePostReply{PostId: result.PostID}, nil
}

// UpdatePost 处理助教编辑知识帖请求。
//
// 成功返回空 reply；归属校验和缓存删除由 Biz/Data 层完成。
func (s *TutorService) UpdatePost(ctx context.Context, req *pb.UpdatePostRequest) (*pb.UpdatePostReply, error) {
	postModel := &model.Post{}

	// 拷贝同名字段 (Title, Content)
	cf.CopyCommonFields(postModel, req)
	// 手动补齐 ID 字段
	postModel.PostID = req.PostId
	postModel.AuthorID = req.TutorId

	// Biz 层会先查原帖确认 author_id，再执行更新。
	err := s.uc.UpdatePost(ctx, postModel)
	if err != nil {
		return nil, err
	}
	return &pb.UpdatePostReply{}, nil
}

// DeletePost 处理助教删除知识帖请求。
//
// 返回空 reply；删除是软删除，data 层会更新 status/deleted_at 并删除帖子缓存。
func (s *TutorService) DeletePost(ctx context.Context, req *pb.DeletePostRequest) (*pb.DeletePostReply, error) {
	// 删除操作仅涉及 ID 透传，直接调用 Biz
	err := s.uc.DeletePost(ctx, req.TutorId, req.PostId)
	return &pb.DeletePostReply{}, err
}

// ListTutorPosts 查询助教自己发布的帖子列表。
//
// 返回帖子分页 items 和 total；data 层负责列表缓存与 MySQL 分页查询。
func (s *TutorService) ListTutorPosts(ctx context.Context, req *pb.ListTutorPostsRequest) (*pb.ListTutorPostsReply, error) {
	// Biz 返回数据库模型和总数，service 再转换成 PostDTO。
	posts, total, err := s.uc.ListTutorPosts(ctx, req.TutorId, req.PageNum, req.PageSize)
	if err != nil {
		return nil, err
	}

	return &pb.ListTutorPostsReply{
		Items: postDTOsFromModels(posts),
		Total: total,
	}, nil
}

// ListPostCommentsTutor 查询助教视角的帖子评论列表。
//
// 返回当前页可见评论和 total；data 层使用列表 ID 缓存 + 评论对象缓存。
func (s *TutorService) ListPostCommentsTutor(ctx context.Context, req *pb.ListPostCommentsRequest) (*pb.ListPostCommentsReply, error) {
	// Biz 只透传分页条件；可见性、缓存和 SQL 由 data 层处理。
	comments, total, err := s.uc.ListPostCommentsTutor(ctx, req.PostId, req.PageNum, req.PageSize)
	if err != nil {
		return nil, err
	}

	return &pb.ListPostCommentsReply{
		Items: commentDTOsWithoutReplies(comments),
		Total: total,
	}, nil
}

// GetCommentDetailTutor 查询助教端评论详情。
//
// 返回评论主体、回复列表、所属帖子上下文，供助教处理评论时查看完整背景。
func (s *TutorService) GetCommentDetailTutor(ctx context.Context, req *pb.GetCommentDetailRequest) (*pb.GetCommentDetailReply, error) {
	// Biz 层负责聚合 comment、replies、post；service 只做 DTO 转换。
	comment, replies, post, err := s.uc.GetCommentDetailTutor(ctx, req.CommentId)
	if err != nil {
		return nil, err
	}

	// 组装最终 Reply，评论和回复字段映射统一交给 mapper。
	reply := &pb.GetCommentDetailReply{
		Comment: commentDTOFromModel(comment, replies),
	}

	// 组装帖子上下文
	if post != nil {
		postInfo := &pb.PostDTO{}
		cf.CopyCommonFields(postInfo, post)
		postInfo.PostId = post.PostID
		postInfo.AuthorId = post.AuthorID
		reply.PostInfo = postInfo
	}

	return reply, nil
}

// DeleteCommentTutor 处理助教删除学生评论请求。
//
// 返回空 reply；Biz 层会校验评论所属帖子是否属于该助教，避免越权。
func (s *TutorService) DeleteCommentTutor(ctx context.Context, req *pb.DeleteCommentTutorRequest) (*pb.DeleteCommentTutorReply, error) {
	// Biz 校验 tutor_id 与 post.author_id 后，data 层软删除评论并扣评论数。
	err := s.uc.DeleteCommentTutor(ctx, req.TutorId, req.CommentId)
	return &pb.DeleteCommentTutorReply{}, err
}

// ReplyComment 处理助教回复评论请求。
//
// 返回新生成的 comment_reply_id；回复 ID、post_id、状态由 Biz 层补齐。
func (s *TutorService) ReplyComment(ctx context.Context, req *pb.ReplyCommentRequest) (*pb.ReplyCommentReply, error) {
	replyModel := &model.StudyCommentReply{}

	cf.CopyCommonFields(replyModel, req) // 拷贝 Content
	replyModel.TutorID = req.TutorId
	replyModel.CommentID = req.CommentId

	// Biz 会先查目标评论，拿到 post_id 后再创建回复。
	result, err := s.uc.ReplyComment(ctx, replyModel)
	if err != nil {
		if errors.Is(err, biz.ErrCommentAlreadyReplied) {
			return nil, kerrors.Conflict("COMMENT_ALREADY_REPLIED", err.Error())
		}
		return nil, err
	}
	return &pb.ReplyCommentReply{CommentReplyId: result.CommentReplyID}, nil
}

// DeleteReply 处理助教撤回自己回复的请求。
//
// 返回空 reply；Biz 层会校验 reply.tutor_id，data 层软删除并清理回复列表缓存。
func (s *TutorService) DeleteReply(ctx context.Context, req *pb.DeleteReplyRequest) (*pb.DeleteReplyReply, error) {
	// 删除回复需要 tutor_id 参与权限校验，不能只按 reply_id 删除。
	err := s.uc.DeleteReply(ctx, req.TutorId, req.CommentReplyId)
	return &pb.DeleteReplyReply{}, err
}

// GetPostDetailTutor 查询助教端帖子详情。
//
// 返回帖子主体、当前页评论、每条评论的回复和 total_comments。
func (s *TutorService) GetPostDetailTutor(ctx context.Context, req *pb.GetPostDetailRequest) (*pb.GetPostDetailReply, error) {
	// 设置默认分页参数
	if req.CommentPageNum <= 0 {
		req.CommentPageNum = 1
	}
	if req.CommentPageSize <= 0 || req.CommentPageSize > 100 {
		req.CommentPageSize = 20
	}

	// Biz 返回帖子、当前页评论和评论总数；回复单独批量查，避免逐条调用。
	post, comments, totalComments, err := s.uc.GetPostDetailTutor(ctx, req.PostId, req.CommentPageNum, req.CommentPageSize)
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
