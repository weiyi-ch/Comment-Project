package task

import "context"

const (
	postCommentDirtyEventType = "post_comment_count_dirty"

	postCommentCountDeltaKey           = "counter:post_comment:delta"
	postCommentCountDirtyKey           = "queue:post_comment:dirty"
	postCommentCountDirtyAtKey         = "queue:post_comment:dirty_at"
	postCommentCountDirtySinceKey      = "queue:post_comment:dirty_since"
	postCommentCountScheduleKey        = "queue:post_comment:flush_schedule"
	postCommentCountProcessingKey      = "counter:post_comment:processing"
	postCommentCountProcessingBatchKey = "counter:post_comment:processing_batch"
)

var postCommentCounterKind = postCounterKind{
	name:               "post_comment",
	eventType:          postCommentDirtyEventType,
	deltaKey:           postCommentCountDeltaKey,
	dirtyKey:           postCommentCountDirtyKey,
	dirtyAtKey:         postCommentCountDirtyAtKey,
	dirtySinceKey:      postCommentCountDirtySinceKey,
	scheduleKey:        postCommentCountScheduleKey,
	processingKey:      postCommentCountProcessingKey,
	processingBatchKey: postCommentCountProcessingBatchKey,
	dbColumn:           "comment_count",
}

func (js *JobWorker) startPostCommentCountFallbackFlusher(ctx context.Context) {
	js.startPostCounterScheduler(ctx, postCommentCounterKind)
}

func (js *JobWorker) flushPostCommentCountDeltas(ctx context.Context, batchSize int64) (int, error) {
	return js.flushPostCounterDeltas(ctx, postCommentCounterKind, batchSize)
}
