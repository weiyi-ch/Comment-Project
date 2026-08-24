package data

import (
	"context"
	"strconv"
	"time"

	"comment-service/dal/model"
)

const (
	// postLikeCountDeltaKey 保存每个帖子的待落库点赞数增量。
	//
	// field 是 post_id，value 是聚合后的 delta；点赞写 +1，取消点赞写 -1。
	postLikeCountDeltaKey = "counter:post_like:delta"
	// postLikeCountDirtyKey 保存有待落库增量的 post_id。
	//
	// service 只负责把帖子标记为 dirty，真正刷库由 comment-task 后台任务完成。
	postLikeCountDirtyKey = "queue:post_like:dirty"
	// postLikeCountDirtyAtKey 记录每个 dirty 帖子最后一次发生点赞/取消点赞的时间。
	//
	// task 会等这个时间超过安静窗口后再刷库，避免压测期间一边写 post_like 一边更新 post 行。
	postLikeCountDirtyAtKey = "queue:post_like:dirty_at"
	// postLikeCountDirtySinceKey 记录本轮 dirty 周期第一次变化时间。
	//
	// task 用 dirty_at + 安静窗口和 dirty_since + 最大等待时间共同计算 nextFlushTime。
	postLikeCountDirtySinceKey = "queue:post_like:dirty_since"
	// postLikeCountProcessingSumKey 保存已被 task 切成 batch、但还没有确认落库的总 delta。
	//
	// 详情读侧只需要 pending + processing_sum，不需要扫描每个 processing_batch。
	postLikeCountProcessingSumKey = "counter:post_like:processing_sum"
	// postLikeCountEnqueueTimeout 限制请求链路等待 Redis 计数入队的时间。
	//
	// post_like 是事实表，Redis 计数是异步冗余计数；远程 Redis 抖动时不能把点赞接口拖成 500。
	postLikeCountEnqueueTimeout = 150 * time.Millisecond
)

const enqueuePostCounterDeltaScript = `
redis.call("HINCRBY", KEYS[1], ARGV[1], ARGV[2])
local added = redis.call("SADD", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
if not redis.call("ZSCORE", KEYS[4], ARGV[1]) then
	redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
end
return added
`

// enqueuePostLikeCountDelta 将点赞数变化写入 Redis，交给 comment-task 异步落库。
//
// delta 可以是 +1 或 -1：大量用户同时点赞/取消同一帖子时，请求链路不会同步更新
// post 表，只做 Redis 原子累加并标记 dirty，避免高频计数 update 污染 post 表 binlog。
func (d *Data) enqueuePostLikeCountDelta(ctx context.Context, postID, delta int64) error {
	if d.cache == nil || delta == 0 {
		return nil
	}

	enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), postLikeCountEnqueueTimeout)
	defer cancel()

	postIDText := strconv.FormatInt(postID, 10)
	raw, err := d.cache.Eval(enqueueCtx, enqueuePostCounterDeltaScript, []string{
		postLikeCountDeltaKey,
		postLikeCountDirtyKey,
		postLikeCountDirtyAtKey,
		postLikeCountDirtySinceKey,
	}, postIDText, strconv.FormatInt(delta, 10), strconv.FormatInt(time.Now().UnixMilli(), 10)).Result()
	if err != nil {
		return err
	}

	// Lua 返回 1 表示该帖子刚从 clean 变 dirty，此时才需要发一条 Kafka 通知。
	// 如果返回 0，说明已有 zset 调度或兜底扫描会处理，不重复发消息。
	if redisInt64(raw) == 1 {
		if err := d.recordCounterDirtyOutbox(enqueueCtx, postLikeDirtyEventType, postID); err != nil {
			// outbox 失败时仍保留 Redis dirty set，comment-task 的定期扫描会兜底发现。
			d.log.WithContext(ctx).Warnf("record post like count dirty outbox failed, post_id=%d, err=%v", postID, err)
		}
	}
	return nil
}

// notifyPostLikeCountDirty 发送 Kafka 脏通知，唤醒 comment-task 处理指定帖子。
func (d *Data) notifyPostLikeCountDirty(ctx context.Context, postID int64) error {
	if d.postLikeDirtyWriter == nil {
		return nil
	}
	return d.postLikeDirtyWriter.Notify(ctx, postID)
}

// pendingPostLikeCountDelta 读取某个帖子的尚未落库点赞数增量。
//
// 读详情时用它叠加 post_counter 中的 like_count，让异步落库窗口内的展示值尽量接近实时。
func (d *Data) pendingPostLikeCountDelta(ctx context.Context, postID int64) int64 {
	if d.cache == nil {
		return 0
	}

	return d.counterDelta(ctx, postLikeCountDeltaKey, postLikeCountProcessingSumKey, postID, "post like")
}

// applyPendingPostLikeCountDelta 把 Redis pending delta 合并到帖子模型上。
//
// post_counter 中的 like_count 是已落库值，Redis 中的 delta 是待刷库值；两者相加后返回给前端。
func (d *Data) applyPendingPostLikeCountDelta(ctx context.Context, post *model.Post) {
	if post == nil {
		return
	}

	delta := d.pendingPostLikeCountDelta(ctx, post.PostID)
	if delta == 0 {
		return
	}

	// 异常情况下如果取消点赞 delta 多于当前库值，展示层兜底不返回负数。
	value := int64(post.LikeCount) + delta
	if value < 0 {
		value = 0
	}
	post.LikeCount = int32(value)
}
