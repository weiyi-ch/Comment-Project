package task

import "context"

const (
	postCommentDirtyEventType = "post_comment_count_dirty"

	postCommentCountDeltaKey             = "counter:post_comment:delta"
	postCommentCountDirtyKey             = "queue:post_comment:dirty"
	postCommentCountDirtyAtKey           = "queue:post_comment:dirty_at"
	postCommentCountDirtySinceKey        = "queue:post_comment:dirty_since"
	postCommentCountScheduleKey          = "queue:post_comment:flush_schedule"
	postCommentCountProcessingBatchKey   = "counter:post_comment:processing_batch"
	postCommentCountProcessingSumKey     = "counter:post_comment:processing_sum"
	postCommentCountProcessingBatchesKey = "counter:post_comment:processing_batches"
)

var postCommentCounterKind = postCounterKind{
	name:                 "post_comment",
	eventType:            postCommentDirtyEventType,
	deltaKey:             postCommentCountDeltaKey,
	dirtyKey:             postCommentCountDirtyKey,
	dirtyAtKey:           postCommentCountDirtyAtKey,
	dirtySinceKey:        postCommentCountDirtySinceKey,
	scheduleKey:          postCommentCountScheduleKey,
	processingBatchKey:   postCommentCountProcessingBatchKey,
	processingSumKey:     postCommentCountProcessingSumKey,
	processingBatchesKey: postCommentCountProcessingBatchesKey,
	dbColumn:             "comment_count",
	factCountSQL:         "SELECT COUNT(*) FROM study_comment WHERE post_id = ? AND visible_status = 1 AND deleted_at IS NULL",
}

func (js *JobWorker) startPostCommentCountFallbackFlusher(ctx context.Context) {
	js.startPostCounterScheduler(ctx, postCommentCounterKind)
}

func (js *JobWorker) flushPostCommentCountDeltas(ctx context.Context, batchSize int64) (int, error) {
	return js.flushPostCounterDeltas(ctx, postCommentCounterKind, batchSize)
}
