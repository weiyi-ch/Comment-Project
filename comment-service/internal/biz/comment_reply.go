package biz

import "comment-service/dal/model"

// replyModelsFromComment 将评论行中内嵌的一次性助教回复转换为兼容现有接口的回复切片。
// 业务上最多返回一条，保留切片只是为了兼容当前 Proto 的 repeated replies 字段。
func replyModelsFromComment(comment *model.StudyComment) []*model.StudyCommentReply {
	if comment == nil || comment.ReplyStatus != 1 || comment.ReplyID <= 0 || comment.ReplyContent == nil {
		return []*model.StudyCommentReply{}
	}

	reply := &model.StudyCommentReply{
		CommentReplyID: comment.ReplyID,
		CommentID:      comment.CommentID,
		TutorID:        comment.ReplyTutorID,
		PostID:         comment.PostID,
		Content:        *comment.ReplyContent,
		Status:         1,
	}
	if comment.RepliedAt != nil {
		reply.CreatedAt = *comment.RepliedAt
	}
	return []*model.StudyCommentReply{reply}
}
