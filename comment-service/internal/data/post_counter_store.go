package data

import (
	"context"
	"errors"
	"fmt"

	"comment-service/dal/model"
	"comment-service/internal/biz"

	"gorm.io/gorm"
)

// postCounter 是 post_counter 表的数据模型。
//
// post_counter 只保存高频变化的冗余统计值；post_like/study_comment 才是事实来源。
// 这样 Canal 后续监听 post 表时，不会被 like_count/comment_count 的高频 update 淹没。
type postCounter struct {
	PostID       int64 `gorm:"column:post_id"`
	LikeCount    int32 `gorm:"column:like_count"`
	CommentCount int32 `gorm:"column:comment_count"`
}

func (postCounter) TableName() string {
	return "post_counter"
}

func (d *Data) queryPostCounterByID(ctx context.Context, postID int64) (*postStatsCache, error) {
	var row postCounter
	err := d.q.Post.WithContext(ctx).UnderlyingDB().
		WithContext(ctx).
		Where("post_id = ?", postID).
		First(&row).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &postStatsCache{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &postStatsCache{
		LikeCount:    row.LikeCount,
		CommentCount: row.CommentCount,
	}, nil
}

func (d *Data) loadPostCounters(ctx context.Context, postIDs []int64) (map[int64]postStatsCache, error) {
	ids := uniquePostIDs(postIDs)
	if len(ids) == 0 {
		return map[int64]postStatsCache{}, nil
	}

	var rows []postCounter
	err := d.q.Post.WithContext(ctx).UnderlyingDB().
		WithContext(ctx).
		Where("post_id IN ?", ids).
		Find(&rows).
		Error
	if err != nil {
		return nil, err
	}

	stats := make(map[int64]postStatsCache, len(rows))
	for _, row := range rows {
		stats[row.PostID] = postStatsCache{
			LikeCount:    row.LikeCount,
			CommentCount: row.CommentCount,
		}
	}
	return stats, nil
}

func (d *Data) attachPersistedPostCounters(ctx context.Context, posts []*model.Post) error {
	if len(posts) == 0 {
		return nil
	}
	postIDs := make([]int64, 0, len(posts))
	for _, post := range posts {
		if post != nil && post.PostID > 0 {
			postIDs = append(postIDs, post.PostID)
		}
	}
	stats, err := d.loadPostCounters(ctx, postIDs)
	if err != nil {
		return fmt.Errorf("load post counters: %w", err)
	}
	for _, post := range posts {
		if post == nil {
			continue
		}
		stat := stats[post.PostID]
		post.LikeCount = stat.LikeCount
		post.CommentCount = stat.CommentCount
	}
	return nil
}

func (d *Data) attachPostCounters(ctx context.Context, posts []*model.Post) error {
	if err := d.attachPersistedPostCounters(ctx, posts); err != nil {
		return err
	}
	for _, post := range posts {
		d.applyPendingPostLikeCountDelta(ctx, post)
		d.applyPendingPostCommentCountDelta(ctx, post)
	}
	return nil
}

func (d *Data) attachBizPostCounters(ctx context.Context, posts []*biz.Post) error {
	if len(posts) == 0 {
		return nil
	}
	postIDs := make([]int64, 0, len(posts))
	for _, post := range posts {
		if post != nil && post.PostID > 0 {
			postIDs = append(postIDs, post.PostID)
		}
	}
	stats, err := d.loadPostCounters(ctx, postIDs)
	if err != nil {
		return fmt.Errorf("load post counters: %w", err)
	}
	for _, post := range posts {
		if post == nil {
			continue
		}
		stat := stats[post.PostID]
		likeCount := int64(stat.LikeCount) + d.pendingPostLikeCountDelta(ctx, post.PostID)
		commentCount := int64(stat.CommentCount) + d.counterDelta(ctx, postCommentCountDeltaKey, postCommentCountProcessingSumKey, post.PostID, "post comment")
		if likeCount < 0 {
			likeCount = 0
		}
		if commentCount < 0 {
			commentCount = 0
		}
		post.LikeCount = int32(likeCount)
		post.CommentCount = int32(commentCount)
	}
	return nil
}

func uniquePostIDs(ids []int64) []int64 {
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
