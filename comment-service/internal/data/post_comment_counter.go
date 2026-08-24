package data

import (
	"context"
	"strconv"
	"time"

	"comment-service/dal/model"
)

const (
	postCommentCountDeltaKey         = "counter:post_comment:delta"
	postCommentCountDirtyKey         = "queue:post_comment:dirty"
	postCommentCountDirtyAtKey       = "queue:post_comment:dirty_at"
	postCommentCountDirtySinceKey    = "queue:post_comment:dirty_since"
	postCommentCountProcessingSumKey = "counter:post_comment:processing_sum"
)

// enqueuePostCommentCountDelta 聚合评论新增、删除和审核驳回产生的计数变化。
func (d *Data) enqueuePostCommentCountDelta(ctx context.Context, postID, delta int64) error {
	if d.cache == nil || delta == 0 {
		return nil
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 150*time.Millisecond)
	defer cancel()

	postIDText := strconv.FormatInt(postID, 10)
	raw, err := d.cache.Eval(enqueueCtx, enqueuePostCounterDeltaScript, []string{
		postCommentCountDeltaKey,
		postCommentCountDirtyKey,
		postCommentCountDirtyAtKey,
		postCommentCountDirtySinceKey,
	}, postIDText, strconv.FormatInt(delta, 10), strconv.FormatInt(time.Now().UnixMilli(), 10)).Result()
	if err != nil {
		return err
	}

	if redisInt64(raw) == 1 {
		if err := d.recordCounterDirtyOutbox(enqueueCtx, postCommentDirtyEventType, postID); err != nil {
			d.log.WithContext(ctx).Warnf("record post comment count dirty outbox failed, post_id=%d, err=%v", postID, err)
		}
	}
	return nil
}

func (d *Data) recordPostCommentCountDelta(ctx context.Context, postID, delta int64) {
	if err := d.enqueuePostCommentCountDelta(ctx, postID, delta); err != nil {
		d.log.WithContext(ctx).Warnf("enqueue post comment count delta failed, post_id=%d, delta=%d, err=%v", postID, delta, err)
		// Redis 异常时退化为同步更新 post_counter，避免评论事实已写入但计数永久遗漏。
		// 注意不要回写 post 表，否则 Canal 监听 post 时仍会收到高频计数 update。
		result := d.q.Post.WithContext(context.WithoutCancel(ctx)).UnderlyingDB().Exec(
			`INSERT INTO post_counter (post_id, like_count, comment_count)
VALUES (?, 0, GREATEST(?, 0))
ON DUPLICATE KEY UPDATE comment_count = GREATEST(comment_count + ?, 0)`,
			postID,
			delta,
			delta,
		)
		if result.Error != nil {
			d.log.WithContext(ctx).Errorf("fallback update post comment count failed, post_id=%d, delta=%d, err=%v", postID, delta, result.Error)
			return
		}
		_ = d.cache.Del(context.WithoutCancel(ctx), buildPostStatsCacheKey(postID)).Err()
	}
}

func (d *Data) applyPendingPostCommentCountDelta(ctx context.Context, post *model.Post) {
	if d.cache == nil || post == nil {
		return
	}
	delta := d.counterDelta(ctx, postCommentCountDeltaKey, postCommentCountProcessingSumKey, post.PostID, "post comment")
	value := int64(post.CommentCount) + delta
	if value < 0 {
		value = 0
	}
	post.CommentCount = int32(value)
}
