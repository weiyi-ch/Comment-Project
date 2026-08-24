package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	postLikeDirtyEventType = "post_like_count_dirty"

	postLikeCountDeltaKey           = "counter:post_like:delta"
	postLikeCountDirtyKey           = "queue:post_like:dirty"
	postLikeCountDirtyAtKey         = "queue:post_like:dirty_at"
	postLikeCountDirtySinceKey      = "queue:post_like:dirty_since"
	postLikeCountScheduleKey        = "queue:post_like:flush_schedule"
	postLikeCountProcessingKey      = "counter:post_like:processing"
	postLikeCountProcessingBatchKey = "counter:post_like:processing_batch"

	postLikeCountFlushQuietPeriod      = 5 * time.Second
	postLikeCountFlushMaxDelay         = 60 * time.Second
	postLikeCountFallbackFlushInterval = 5 * time.Second
	postLikeCountScheduleScanInterval  = 1 * time.Second
	postLikeCountProcessingRetryDelay  = 3 * time.Second
	postLikeCountFlushBatchSize        = 100
	postLikeCountFallbackScheduleSize  = 100
	postStatsCachePrefix               = "mysql:post:stats:"
	postCounterTableDDL                = "CREATE TABLE IF NOT EXISTS post_counter (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, post_id BIGINT NOT NULL, like_count INT NOT NULL DEFAULT 0, comment_count INT NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE KEY uk_post_id (post_id), KEY idx_like_count (like_count), KEY idx_comment_count (comment_count))"
	counterFlushLogTableDDL            = "CREATE TABLE IF NOT EXISTS counter_flush_log (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, batch_id VARCHAR(128) NOT NULL, counter_type VARCHAR(32) NOT NULL, post_id BIGINT NOT NULL, delta BIGINT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE KEY uk_counter_flush_batch_id (batch_id))"
	counterFlushLogInsertSQL           = "INSERT INTO counter_flush_log(batch_id, counter_type, post_id, delta) VALUES (?, ?, ?, ?)"
)

type postLikeDirtyMessage struct {
	Type   string `json:"type"`
	PostID string `json:"post_id"`
}

type postCounterKind struct {
	name               string
	eventType          string
	deltaKey           string
	dirtyKey           string
	dirtyAtKey         string
	dirtySinceKey      string
	scheduleKey        string
	processingKey      string
	processingBatchKey string
	dbColumn           string
}

var (
	postLikeCounterKind = postCounterKind{
		name:               "post_like",
		eventType:          postLikeDirtyEventType,
		deltaKey:           postLikeCountDeltaKey,
		dirtyKey:           postLikeCountDirtyKey,
		dirtyAtKey:         postLikeCountDirtyAtKey,
		dirtySinceKey:      postLikeCountDirtySinceKey,
		scheduleKey:        postLikeCountScheduleKey,
		processingKey:      postLikeCountProcessingKey,
		processingBatchKey: postLikeCountProcessingBatchKey,
		dbColumn:           "like_count",
	}
	counterFlushLogOnce sync.Once
	counterFlushLogErr  error
)

const claimPostCounterDeltaScript = `
if redis.call("HEXISTS", KEYS[5], ARGV[1]) == 1 then
	return "__processing__"
end

local dirty_at = redis.call("ZSCORE", KEYS[3], ARGV[1])
if dirty_at then
	local dirty_since = redis.call("ZSCORE", KEYS[4], ARGV[1])
	if not dirty_since then
		dirty_since = dirty_at
	end

	local next_flush = tonumber(dirty_at) + tonumber(ARGV[2])
	local max_flush = tonumber(dirty_since) + tonumber(ARGV[3])
	if max_flush < next_flush then
		next_flush = max_flush
	end

	if next_flush > tonumber(ARGV[4]) then
		redis.call("ZADD", KEYS[7], next_flush, ARGV[1])
		return "__not_ready__"
	end
end

local delta = redis.call("HGET", KEYS[1], ARGV[1])
if not delta then
	redis.call("SREM", KEYS[2], ARGV[1])
	redis.call("ZREM", KEYS[3], ARGV[1])
	redis.call("ZREM", KEYS[4], ARGV[1])
	redis.call("ZREM", KEYS[7], ARGV[1])
	return 0
end

redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("HSET", KEYS[5], ARGV[1], delta)
redis.call("HSET", KEYS[6], ARGV[1], ARGV[5])
redis.call("SREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[7], ARGV[1])
return delta
`

func (js *JobWorker) startPostLikeCountFallbackFlusher(ctx context.Context) {
	js.startPostCounterScheduler(ctx, postLikeCounterKind)
}

func (js *JobWorker) startPostLikeCountKafkaConsumer(ctx context.Context) {
	if js == nil {
		return
	}
	if js.postLikeReader == nil {
		js.log.Warn("post counter kafka consumer skipped: reader is not ready")
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			m, err := js.postLikeReader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					js.log.Errorf("read post counter dirty kafka message failed: %v", err)
				}
				return
			}

			if err := js.handlePostLikeCountDirtyMessage(ctx, m.Value); err != nil {
				js.log.Errorf("handle post counter dirty kafka message failed: %v, raw=%s", err, string(m.Value))
			}
		}
	}()
}

func (js *JobWorker) handlePostLikeCountDirtyMessage(ctx context.Context, raw []byte) error {
	var msg postLikeDirtyMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}

	postID, err := strconv.ParseInt(msg.PostID, 10, 64)
	if err != nil || postID <= 0 {
		return fmt.Errorf("invalid post counter dirty post_id=%q", msg.PostID)
	}

	switch msg.Type {
	case postLikeDirtyEventType:
		return js.schedulePostCounterFlush(ctx, postLikeCounterKind, msg.PostID)
	case postCommentDirtyEventType:
		return js.schedulePostCounterFlush(ctx, postCommentCounterKind, msg.PostID)
	default:
		return fmt.Errorf("unsupported post counter dirty event type=%q", msg.Type)
	}
}

func (js *JobWorker) startPostCounterScheduler(ctx context.Context, kind postCounterKind) {
	if js == nil {
		return
	}
	if js.data == nil || js.data.Redis() == nil || js.data.DB() == nil {
		js.log.Warnf("%s counter scheduler skipped: data resources are not ready", kind.name)
		return
	}

	go func() {
		scanTicker := time.NewTicker(postLikeCountScheduleScanInterval)
		fallbackTicker := time.NewTicker(postLikeCountFallbackFlushInterval)
		defer scanTicker.Stop()
		defer fallbackTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-scanTicker.C:
				flushed, err := js.flushPostCounterDeltas(ctx, kind, postLikeCountFlushBatchSize)
				if err != nil && ctx.Err() == nil {
					js.log.Warnf("flush %s counter deltas failed: %v", kind.name, err)
				}
				if flushed > 0 {
					js.log.Debugf("flush %s counter deltas success, count=%d", kind.name, flushed)
				}
			case <-fallbackTicker.C:
				if err := js.scheduleDirtyPostCountersFromSet(ctx, kind, postLikeCountFallbackScheduleSize); err != nil && ctx.Err() == nil {
					js.log.Warnf("fallback schedule %s dirty counters failed: %v", kind.name, err)
				}
			}
		}
	}()
}

func (js *JobWorker) scheduleDirtyPostCountersFromSet(ctx context.Context, kind postCounterKind, batchSize int64) error {
	if batchSize <= 0 {
		batchSize = postLikeCountFallbackScheduleSize
	}
	postIDs, err := js.data.Redis().SRandMemberN(ctx, kind.dirtyKey, batchSize).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	for _, postIDText := range postIDs {
		if _, err := strconv.ParseInt(postIDText, 10, 64); err != nil {
			js.log.Warnf("skip invalid %s dirty id=%s", kind.name, postIDText)
			_ = js.data.Redis().SRem(ctx, kind.dirtyKey, postIDText).Err()
			_ = js.data.Redis().ZRem(ctx, kind.dirtyAtKey, postIDText).Err()
			_ = js.data.Redis().ZRem(ctx, kind.dirtySinceKey, postIDText).Err()
			_ = js.data.Redis().ZRem(ctx, kind.scheduleKey, postIDText).Err()
			continue
		}
		if err := js.schedulePostCounterFlush(ctx, kind, postIDText); err != nil {
			return err
		}
	}
	return nil
}

func (js *JobWorker) schedulePostCounterFlush(ctx context.Context, kind postCounterKind, postIDText string) error {
	cache := js.data.Redis()
	dirtyAt, err := cache.ZScore(ctx, kind.dirtyAtKey, postIDText).Result()
	if err != nil {
		if err == redis.Nil {
			_ = cache.SRem(ctx, kind.dirtyKey, postIDText).Err()
			_ = cache.ZRem(ctx, kind.dirtySinceKey, postIDText).Err()
			_ = cache.ZRem(ctx, kind.scheduleKey, postIDText).Err()
			return nil
		}
		return err
	}

	dirtySince, err := cache.ZScore(ctx, kind.dirtySinceKey, postIDText).Result()
	if err != nil {
		if err != redis.Nil {
			return err
		}
		dirtySince = dirtyAt
		_ = cache.ZAdd(ctx, kind.dirtySinceKey, redis.Z{Score: dirtySince, Member: postIDText}).Err()
	}

	nextFlushMillis := nextPostCounterFlushMillis(int64(dirtyAt), int64(dirtySince))
	return cache.ZAdd(ctx, kind.scheduleKey, redis.Z{
		Score:  float64(nextFlushMillis),
		Member: postIDText,
	}).Err()
}

func (js *JobWorker) flushPostLikeCountDeltas(ctx context.Context, batchSize int64) (int, error) {
	return js.flushPostCounterDeltas(ctx, postLikeCounterKind, batchSize)
}

func (js *JobWorker) flushPostCounterDeltas(ctx context.Context, kind postCounterKind, batchSize int64) (int, error) {
	if js == nil || js.data == nil || js.data.Redis() == nil {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = postLikeCountFlushBatchSize
	}

	nowMillis := time.Now().UnixMilli()
	postIDs, err := js.data.Redis().ZRangeByScore(ctx, kind.scheduleKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(nowMillis, 10),
		Count: batchSize,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}

	flushed := 0
	for _, postIDText := range postIDs {
		postID, err := strconv.ParseInt(postIDText, 10, 64)
		if err != nil {
			js.log.Warnf("skip invalid %s scheduled id=%s", kind.name, postIDText)
			_ = js.data.Redis().ZRem(ctx, kind.scheduleKey, postIDText).Err()
			continue
		}

		ok, err := js.flushPostCounterByPostID(ctx, kind, postIDText, postID)
		if err != nil {
			return flushed, err
		}
		if ok {
			flushed++
		}
	}
	return flushed, nil
}

func (js *JobWorker) flushPostCounterByPostID(ctx context.Context, kind postCounterKind, postIDText string, postID int64) (bool, error) {
	if ok, err := js.flushProcessingPostCounter(ctx, kind, postIDText, postID); ok || err != nil {
		return ok, err
	}

	if nextFlushMillis, ready, err := js.recheckPostCounterReady(ctx, kind, postIDText); err != nil {
		return false, err
	} else if !ready {
		_ = js.data.Redis().ZAdd(ctx, kind.scheduleKey, redis.Z{
			Score:  float64(nextFlushMillis),
			Member: postIDText,
		}).Err()
		return false, nil
	}

	batchID := newCounterBatchID(kind.name, postIDText)
	delta, state, err := js.claimPostCounterDelta(ctx, kind, postIDText, batchID)
	if err != nil {
		return false, err
	}
	switch state {
	case "not_ready":
		return false, nil
	case "processing":
		_, err := js.flushProcessingPostCounter(ctx, kind, postIDText, postID)
		return false, err
	case "empty":
		return false, nil
	}

	if err := js.applyPostCounterDeltaToDB(ctx, kind, postID, delta, batchID); err != nil {
		_ = js.data.Redis().ZAdd(context.Background(), kind.scheduleKey, redis.Z{
			Score:  float64(time.Now().Add(postLikeCountProcessingRetryDelay).UnixMilli()),
			Member: postIDText,
		}).Err()
		return false, err
	}

	js.clearProcessingPostCounter(ctx, kind, postIDText)
	_ = js.data.Redis().Del(ctx, buildPostStatsCacheKey(postID)).Err()
	return true, nil
}

func (js *JobWorker) flushProcessingPostCounter(ctx context.Context, kind postCounterKind, postIDText string, postID int64) (bool, error) {
	cache := js.data.Redis()
	delta, err := cache.HGet(ctx, kind.processingKey, postIDText).Int64()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	batchID, err := cache.HGet(ctx, kind.processingBatchKey, postIDText).Result()
	if err != nil {
		if err != redis.Nil {
			return false, err
		}
		batchID = newCounterBatchID(kind.name, postIDText)
		_ = cache.HSet(ctx, kind.processingBatchKey, postIDText, batchID).Err()
	}

	if err := js.applyPostCounterDeltaToDB(ctx, kind, postID, delta, batchID); err != nil {
		_ = cache.ZAdd(context.Background(), kind.scheduleKey, redis.Z{
			Score:  float64(time.Now().Add(postLikeCountProcessingRetryDelay).UnixMilli()),
			Member: postIDText,
		}).Err()
		return false, err
	}

	js.clearProcessingPostCounter(ctx, kind, postIDText)
	_ = cache.Del(ctx, buildPostStatsCacheKey(postID)).Err()
	return true, nil
}

func (js *JobWorker) recheckPostCounterReady(ctx context.Context, kind postCounterKind, postIDText string) (int64, bool, error) {
	cache := js.data.Redis()
	dirtyAt, err := cache.ZScore(ctx, kind.dirtyAtKey, postIDText).Result()
	if err != nil {
		if err == redis.Nil {
			_ = cache.ZRem(ctx, kind.scheduleKey, postIDText).Err()
			return 0, true, nil
		}
		return 0, false, err
	}

	dirtySince, err := cache.ZScore(ctx, kind.dirtySinceKey, postIDText).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, false, err
		}
		dirtySince = dirtyAt
	}

	nextFlushMillis := nextPostCounterFlushMillis(int64(dirtyAt), int64(dirtySince))
	return nextFlushMillis, nextFlushMillis <= time.Now().UnixMilli(), nil
}

func (js *JobWorker) claimPostCounterDelta(ctx context.Context, kind postCounterKind, postIDText, batchID string) (int64, string, error) {
	raw, err := js.data.Redis().Eval(ctx, claimPostCounterDeltaScript, []string{
		kind.deltaKey,
		kind.dirtyKey,
		kind.dirtyAtKey,
		kind.dirtySinceKey,
		kind.processingKey,
		kind.processingBatchKey,
		kind.scheduleKey,
	}, postIDText,
		strconv.FormatInt(postLikeCountFlushQuietPeriod.Milliseconds(), 10),
		strconv.FormatInt(postLikeCountFlushMaxDelay.Milliseconds(), 10),
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		batchID,
	).Result()
	if err != nil {
		return 0, "", err
	}

	switch v := raw.(type) {
	case int64:
		if v == 0 {
			return 0, "empty", nil
		}
		return v, "claimed", nil
	case string:
		return parseClaimPostCounterResult(v)
	case []byte:
		return parseClaimPostCounterResult(string(v))
	default:
		return 0, "", fmt.Errorf("unexpected %s counter delta type %T", kind.name, raw)
	}
}

func parseClaimPostCounterResult(value string) (int64, string, error) {
	switch value {
	case "__not_ready__":
		return 0, "not_ready", nil
	case "__processing__":
		return 0, "processing", nil
	default:
		delta, err := strconv.ParseInt(value, 10, 64)
		if delta == 0 && err == nil {
			return 0, "empty", nil
		}
		return delta, "claimed", err
	}
}

func (js *JobWorker) applyPostCounterDeltaToDB(ctx context.Context, kind postCounterKind, postID, delta int64, batchID string) error {
	if delta == 0 {
		return nil
	}
	if js.data.DB() == nil {
		return fmt.Errorf("db is not ready")
	}
	if err := ensureCounterFlushLogTable(js.data.DB()); err != nil {
		return err
	}

	return js.data.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(counterFlushLogInsertSQL, batchID, kind.name, postID, delta).Error; err != nil {
			if isDuplicateCounterBatchError(err) {
				return nil
			}
			return err
		}

		// 计数刷库只更新 post_counter，绝不更新 post。
		// 后续 Canal 可以只监听 post/study_comment 的内容变化，避免热点点赞和评论数 update 冲击 ES 同步链路。
		insertLikeCount, insertCommentCount := initialPostCounterValues(kind, delta)
		sql := fmt.Sprintf(`INSERT INTO post_counter (post_id, like_count, comment_count)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE %s = GREATEST(%s + ?, 0)`, kind.dbColumn, kind.dbColumn)
		return tx.Exec(sql, postID, insertLikeCount, insertCommentCount, delta).Error
	})
}

func initialPostCounterValues(kind postCounterKind, delta int64) (int64, int64) {
	if delta < 0 {
		return 0, 0
	}
	switch kind.dbColumn {
	case "like_count":
		return delta, 0
	case "comment_count":
		return 0, delta
	default:
		return 0, 0
	}
}

func ensureCounterFlushLogTable(db *gorm.DB) error {
	counterFlushLogOnce.Do(func() {
		if err := db.Exec(postCounterTableDDL).Error; err != nil {
			counterFlushLogErr = err
			return
		}
		counterFlushLogErr = db.Exec(counterFlushLogTableDDL).Error
	})
	return counterFlushLogErr
}

func isDuplicateCounterBatchError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "1062")
}

func (js *JobWorker) clearProcessingPostCounter(ctx context.Context, kind postCounterKind, postIDText string) {
	cache := js.data.Redis()
	pipe := cache.TxPipeline()
	pipe.HDel(ctx, kind.processingKey, postIDText)
	pipe.HDel(ctx, kind.processingBatchKey, postIDText)
	_, _ = pipe.Exec(ctx)
}

func nextPostCounterFlushMillis(dirtyAtMillis, dirtySinceMillis int64) int64 {
	quietFlush := dirtyAtMillis + postLikeCountFlushQuietPeriod.Milliseconds()
	maxFlush := dirtySinceMillis + postLikeCountFlushMaxDelay.Milliseconds()
	if maxFlush < quietFlush {
		return maxFlush
	}
	return quietFlush
}

func newCounterBatchID(counterType, postIDText string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err == nil {
		return fmt.Sprintf("%s:%s:%d:%s", counterType, postIDText, time.Now().UnixNano(), hex.EncodeToString(buf))
	}
	return fmt.Sprintf("%s:%s:%d", counterType, postIDText, time.Now().UnixNano())
}

func buildPostStatsCacheKey(postID int64) string {
	return postStatsCachePrefix + strconv.FormatInt(postID, 10)
}
