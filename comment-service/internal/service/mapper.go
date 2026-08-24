package service

import (
	pb "comment-service/api/comment/v1"
	"comment-service/dal/model"
)

// postDTOFromModel 统一把数据库 Post 模型转换成对外返回的 PostDTO。
//
// service 层会在学生、助教、运营多个端复用这个方法，避免每个端各自手写字段映射后出现遗漏。
func postDTOFromModel(post *model.Post) *pb.PostDTO {
	if post == nil {
		return nil
	}

	// 返回给前端的时间统一转毫秒时间戳；数据库模型内部仍保留 time.Time。
	return &pb.PostDTO{
		PostId:       post.PostID,
		AuthorId:     post.AuthorID,
		Title:        post.Title,
		Content:      post.Content,
		Status:       post.Status,
		LikeCount:    post.LikeCount,
		CommentCount: post.CommentCount,
		CreatedAt:    post.CreatedAt.UnixMilli(),
	}
}

// postDTOsFromModels 批量转换帖子模型。
//
// 返回顺序与输入 posts 一致，nil 元素会被跳过。
func postDTOsFromModels(posts []*model.Post) []*pb.PostDTO {
	items := make([]*pb.PostDTO, 0, len(posts))
	for _, post := range posts {
		if dto := postDTOFromModel(post); dto != nil {
			items = append(items, dto)
		}
	}
	return items
}

// commentDTOFromModel 组装评论主体及其回复。
//
// replies 由调用方提前查好；详情页会批量查回复，避免在这里隐藏 N+1 查询。
func commentDTOFromModel(comment *model.StudyComment, replies []*model.StudyCommentReply) *pb.CommentDTO {
	dto := commentDTOWithoutReplies(comment)
	if dto == nil {
		return nil
	}

	// 帖子详情不再额外查询回复表；nil 表示直接使用评论行中内嵌的一次性回复。
	if replies == nil {
		replies = embeddedReplyModelsFromComment(comment)
	}
	dto.Replies = replyDTOsFromModels(replies)
	return dto
}

// commentDTOWithoutReplies 只转换评论主体，不挂载回复列表。
//
// 普通评论列表和搜索结果不返回 replies，使用该 helper 可以避免重复手写字段映射。
func commentDTOWithoutReplies(comment *model.StudyComment) *pb.CommentDTO {
	if comment == nil {
		return nil
	}

	return &pb.CommentDTO{
		CommentId:     comment.CommentID,
		PostId:        comment.PostID,
		StudentId:     comment.StudentID,
		Content:       comment.Content,
		VisibleStatus: comment.VisibleStatus,
		AuditStatus:   comment.AuditStatus,
		CreatedAt:     comment.CreatedAt.UnixMilli(),
	}
}

func embeddedReplyModelsFromComment(comment *model.StudyComment) []*model.StudyCommentReply {
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

// commentDTOsFromModels 按 comment_id 把批量查询到的回复挂回对应评论。
func commentDTOsFromModels(
	comments []*model.StudyComment,
	repliesByCommentID map[int64][]*model.StudyCommentReply,
) []*pb.CommentDTO {
	items := make([]*pb.CommentDTO, 0, len(comments))
	for _, comment := range comments {
		if comment == nil {
			continue
		}
		// repliesByCommentID 的 value 是该评论的回复切片；没有回复时会得到 nil，DTO 中表现为空数组/空值。
		items = append(items, commentDTOFromModel(comment, repliesByCommentID[comment.CommentID]))
	}
	return items
}

// commentDTOsWithoutReplies 批量转换不带回复的评论列表。
//
// 返回顺序与输入 comments 一致，nil 元素会被跳过。
func commentDTOsWithoutReplies(comments []*model.StudyComment) []*pb.CommentDTO {
	items := make([]*pb.CommentDTO, 0, len(comments))
	for _, comment := range comments {
		if dto := commentDTOWithoutReplies(comment); dto != nil {
			items = append(items, dto)
		}
	}
	return items
}

// replyDTOsFromModels 只做字段映射，不做权限或状态判断；这些规则应留在 biz/data 层。
func replyDTOsFromModels(replies []*model.StudyCommentReply) []*pb.ReplyDTO {
	items := make([]*pb.ReplyDTO, 0, len(replies))
	for _, reply := range replies {
		if reply == nil {
			continue
		}
		// 只暴露回复 ID、所属评论、助教 ID、内容和创建时间。
		items = append(items, &pb.ReplyDTO{
			CommentReplyId: reply.CommentReplyID,
			CommentId:      reply.CommentID,
			TutorId:        reply.TutorID,
			Content:        reply.Content,
			CreatedAt:      reply.CreatedAt.UnixMilli(),
		})
	}
	return items
}

// collectCommentIDs 收集当前页评论 ID，供详情页批量加载回复使用。
func collectCommentIDs(comments []*model.StudyComment) []int64 {
	ids := make([]int64, 0, len(comments))
	seen := make(map[int64]struct{}, len(comments))
	for _, comment := range comments {
		if comment == nil || comment.CommentID <= 0 {
			continue
		}
		if _, ok := seen[comment.CommentID]; ok {
			continue
		}
		// seen 用于去重，避免异常重复数据导致批量查回复时重复拼 key。
		seen[comment.CommentID] = struct{}{}
		ids = append(ids, comment.CommentID)
	}
	return ids
}
