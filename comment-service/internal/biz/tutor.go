package biz

import (
	"context"
	"errors"

	"comment-service/dal/model"
	"comment-service/pkg/snowflake"

	"github.com/go-kratos/kratos/v2/log"
)

var ErrCommentAlreadyReplied = errors.New("该评论已经回复，请勿重复操作")

// TutorRepo 数据操作接口定义
type TutorRepo interface {
	// 知识帖操作
	CreatePost(ctx context.Context, post *model.Post) (*model.Post, error)
	UpdatePost(ctx context.Context, post *model.Post) error
	DeletePost(ctx context.Context, postID int64) error
	//Post需要有拼接操作
	GetPostByID(ctx context.Context, postID int64) (*model.Post, error)
	ListTutorPosts(ctx context.Context, tutorID int64, pageNum, pageSize int32) ([]*model.Post, int64, error)

	// 评论与回复操作
	ListPostComments(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error)
	GetCommentByID(ctx context.Context, commentID int64) (*model.StudyComment, error)
	DeleteComment(ctx context.Context, commentID int64) error

	CreateCommentReply(ctx context.Context, reply *model.StudyCommentReply) (*model.StudyCommentReply, error)
	GetReplyByID(ctx context.Context, replyID int64) (*model.StudyCommentReply, error)
	DeleteReply(ctx context.Context, replyID, deletedBy int64) error

	// 帖子详情查询（带分页评论）
	GetPostDetailWithComments(ctx context.Context, postID int64, pageNum, pageSize int32) (*model.Post, []*model.StudyComment, int64, error)
}

// TutorUsecase 封装助教端业务规则。
//
// 这里负责帖子归属校验、回复归属校验、帖子/回复 ID 初始化和聚合读编排。
type TutorUsecase struct {
	repo TutorRepo
	log  *log.Helper
}

// NewTutorUsecase 创建助教端业务用例。
func NewTutorUsecase(repo TutorRepo, logger log.Logger) *TutorUsecase {
	return &TutorUsecase{
		repo: repo,
		log:  log.NewHelper(log.With(logger, "module", "usecase/tutor")),
	}
}

// CreatePost 发布知识帖。
//
// 返回创建后的帖子模型；post_id 和 status 在 Biz 层初始化，data 层只负责落库。
func (uc *TutorUsecase) CreatePost(ctx context.Context, post *model.Post) (*model.Post, error) {
	post.PostID = snowflake.GenID() // 雪花算法生成全局唯一ID
	post.Status = 1                 // 默认状态 1:已发布
	// repo.CreatePost 返回已写入的帖子模型，失败时返回 MySQL/缓存相关错误。
	return uc.repo.CreatePost(ctx, post)
}

// UpdatePost 更新助教自己的知识帖。
//
// 返回 nil 表示更新成功；如果帖子不存在或不属于当前助教，返回业务错误。
func (uc *TutorUsecase) UpdatePost(ctx context.Context, post *model.Post) error {
	// 先查旧帖子，返回值用于确认帖子存在并校验 author_id。
	existingPost, err := uc.repo.GetPostByID(ctx, post.PostID)
	if err != nil || existingPost == nil {
		return errors.New("知识帖不存在")
	}
	if existingPost.AuthorID != post.AuthorID {
		return errors.New("无权编辑他人的知识帖")
	}
	// 校验通过后 data 层只更新标题/内容并删除帖子对象缓存。
	return uc.repo.UpdatePost(ctx, post)
}

// DeletePost 删除助教自己的知识帖。
//
// 删除是软删除；返回 nil 表示删除成功或状态更新成功。
func (uc *TutorUsecase) DeletePost(ctx context.Context, tutorID, postID int64) error {
	// 先查帖子用于校验 author_id，防止助教删除他人帖子。
	existingPost, err := uc.repo.GetPostByID(ctx, postID)
	if err != nil || existingPost == nil {
		return errors.New("知识帖不存在")
	}
	if existingPost.AuthorID != tutorID {
		return errors.New("无权删除他人的知识帖")
	}
	// data 层更新 status/deleted_at，并删除帖子对象缓存。
	return uc.repo.DeletePost(ctx, postID)
}

// ListTutorPosts 查询助教发布的帖子分页。
//
// 返回帖子列表、total 和错误；data 层处理分页与列表缓存。
func (uc *TutorUsecase) ListTutorPosts(ctx context.Context, tutorID int64, pageNum, pageSize int32) ([]*model.Post, int64, error) {
	// repo 按 tutor_id 查询帖子列表，返回当前页和总数。
	return uc.repo.ListTutorPosts(ctx, tutorID, pageNum, pageSize)
}

// ListPostCommentsTutor 查询帖子下助教可见的评论分页。
//
// 返回评论列表、total 和错误。
func (uc *TutorUsecase) ListPostCommentsTutor(ctx context.Context, postID int64, pageNum, pageSize int32) ([]*model.StudyComment, int64, error) {
	// repo 会走评论列表 ID 缓存、评论对象缓存和 MySQL 回源。
	return uc.repo.ListPostComments(ctx, postID, pageNum, pageSize)
}

// GetCommentDetailTutor 查询评论详情。
//
// 返回评论主体、回复列表和所属帖子，用于助教查看评论上下文。
func (uc *TutorUsecase) GetCommentDetailTutor(ctx context.Context, commentID int64) (*model.StudyComment, []*model.StudyCommentReply, *model.Post, error) {
	// 先查评论主体；返回的 PostID 用于继续查所属帖子。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return nil, nil, nil, errors.New("评论不存在")
	}
	// 一次性助教回复已经内嵌在评论行中。
	replies := replyModelsFromComment(comment)

	// 帖子摘要用于提供上下文，失败时返回 nil post。
	post, _ := uc.repo.GetPostByID(ctx, comment.PostID)

	return comment, replies, post, nil
}

// DeleteCommentTutor 删除自己帖子下的学生评论。
//
// 返回 nil 表示软删除成功；会先校验评论所属帖子是否属于当前助教。
func (uc *TutorUsecase) DeleteCommentTutor(ctx context.Context, tutorID, commentID int64) error {
	// 查评论拿到 post_id；如果评论不存在，不能继续删除。
	comment, err := uc.repo.GetCommentByID(ctx, commentID)
	if err != nil || comment == nil {
		return errors.New("评论不存在")
	}

	// 查帖子用于判断 author_id 是否等于当前 tutorID。
	post, err := uc.repo.GetPostByID(ctx, comment.PostID)
	if err != nil || post == nil {
		return errors.New("关联的知识帖不存在")
	}

	if post.AuthorID != tutorID {
		return errors.New("无权删除非本人知识帖下的评论")
	}

	// 校验通过后 data 层软删除评论、扣减 comment_count、删除相关缓存。
	return uc.repo.DeleteComment(ctx, commentID)
}

// ReplyComment 创建助教回复。
//
// 返回创建后的回复模型，包含新生成的 comment_reply_id。
func (uc *TutorUsecase) ReplyComment(ctx context.Context, reply *model.StudyCommentReply) (*model.StudyCommentReply, error) {
	// 先查目标评论，返回值用于确认评论存在并补齐 reply.PostID。
	comment, err := uc.repo.GetCommentByID(ctx, reply.CommentID)
	if err != nil || comment == nil {
		return nil, errors.New("目标评论不存在")
	}

	// 只有帖子作者可以回复该帖子下的学生评论。
	post, err := uc.repo.GetPostByID(ctx, comment.PostID)
	if err != nil || post == nil {
		return nil, errors.New("关联的知识帖不存在")
	}
	if post.AuthorID != reply.TutorID {
		return nil, errors.New("无权回复非本人知识帖下的评论")
	}

	if comment.ReplyStatus != 0 {
		return nil, ErrCommentAlreadyReplied
	}

	reply.PostID = comment.PostID
	reply.CommentReplyID = snowflake.GenID()
	reply.Status = 1

	// data 层写入回复，并删除该评论的回复列表缓存。
	return uc.repo.CreateCommentReply(ctx, reply)
}

// DeleteReply 删除助教自己的回复。
//
// 返回 nil 表示软删除成功；会校验 reply.tutor_id 防止删除他人回复。
func (uc *TutorUsecase) DeleteReply(ctx context.Context, tutorID, replyID int64) error {
	// 先查回复主体，返回的 TutorID 用于权限校验。
	reply, err := uc.repo.GetReplyByID(ctx, replyID)
	if err != nil || reply == nil {
		return errors.New("回复不存在")
	}
	if reply.TutorID != tutorID {
		return errors.New("无权删除他人的回复")
	}

	// deleted_by 记录执行删除的助教 ID，方便后续审计和排查。
	return uc.repo.DeleteReply(ctx, replyID, tutorID)
}

// GetPostDetailTutor 查询助教端帖子详情。
//
// 返回帖子、当前页评论、total_comments 和错误。
func (uc *TutorUsecase) GetPostDetailTutor(ctx context.Context, postID int64, pageNum, pageSize int32) (*model.Post, []*model.StudyComment, int64, error) {
	// 参数校验和默认值设置
	if pageNum <= 0 {
		pageNum = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	// repo 查询出的评论模型已经包含一次性助教回复字段。
	return uc.repo.GetPostDetailWithComments(ctx, postID, pageNum, pageSize)
}
