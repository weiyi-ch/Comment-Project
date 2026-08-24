package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	postLikeCountDeltaKey             = "counter:post_like:delta"
	postLikeCountDirtyKey             = "queue:post_like:dirty"
	postLikeCountDirtyAtKey           = "queue:post_like:dirty_at"
	postLikeCountDirtySinceKey        = "queue:post_like:dirty_since"
	postLikeCountScheduleKey          = "queue:post_like:flush_schedule"
	postLikeCountProcessingBatchKey   = "counter:post_like:processing_batch"
	postLikeCountProcessingSumKey     = "counter:post_like:processing_sum"
	postLikeCountProcessingBatchesKey = "counter:post_like:processing_batches"

	postLikeCountFlushQuietPeriod      = 5 * time.Second
	postLikeCountFlushMaxDelay         = 60 * time.Second
	postLikeCountFallbackFlushInterval = 5 * time.Second
	postLikeCountScheduleScanInterval  = 1 * time.Second
	postLikeCountProcessingRetryDelay  = 3 * time.Second
	postLikeCountFlushBatchSize        = 100
	postLikeCountFallbackScheduleSize  = 100
	postCounterBatchRetryDelay         = 30 * time.Second
	postCounterBatchScanInterval       = 3 * time.Second
	postCounterBatchScanSize           = 100
	postCounterBatchRecoveryScanSize   = 100
	postCounterReconcileInterval       = 10 * time.Minute
	postCounterReconcileBatchSize      = 200
	postStatsCachePrefix               = "mysql:post:stats:"
	postCounterTableDDL                = "CREATE TABLE IF NOT EXISTS post_counter (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, post_id BIGINT NOT NULL, like_count INT NOT NULL DEFAULT 0, comment_count INT NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE KEY uk_post_id (post_id), KEY idx_like_count (like_count), KEY idx_comment_count (comment_count))"
	counterBatchTableDDL               = "CREATE TABLE IF NOT EXISTS counter_batch (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, batch_id VARCHAR(128) NOT NULL, counter_type VARCHAR(32) NOT NULL, post_id BIGINT NOT NULL, delta BIGINT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'pending', retry_count INT NOT NULL DEFAULT 0, next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, last_error VARCHAR(512) NOT NULL DEFAULT '', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE KEY uk_counter_batch_id (batch_id), KEY idx_counter_batch_status_retry (status, next_retry_at), KEY idx_counter_batch_post_status (post_id, counter_type, status))"
	counterReconcileLogTableDDL        = "CREATE TABLE IF NOT EXISTS counter_reconcile_log (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, post_id BIGINT NOT NULL, counter_type VARCHAR(32) NOT NULL, old_count BIGINT NOT NULL, fact_count BIGINT NOT NULL, diff BIGINT NOT NULL, status VARCHAR(32) NOT NULL, reason VARCHAR(255) NOT NULL DEFAULT '', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, KEY idx_post_type_time (post_id, counter_type, created_at), KEY idx_created_at (created_at))"
	counterBatchInsertSQL              = "INSERT INTO counter_batch(batch_id, counter_type, post_id, delta, status, retry_count, next_retry_at) VALUES (?, ?, ?, ?, 'pending', 0, NOW()) ON DUPLICATE KEY UPDATE updated_at = NOW()"
)

type postLikeDirtyMessage struct {
	Type   string `json:"type"`
	PostID string `json:"post_id"`
}

type postCounterKind struct {
	name                 string
	eventType            string
	deltaKey             string
	dirtyKey             string
	dirtyAtKey           string
	dirtySinceKey        string
	scheduleKey          string
	processingBatchKey   string
	processingSumKey     string
	processingBatchesKey string
	dbColumn             string
	factCountSQL         string
}

var (
	postLikeCounterKind = postCounterKind{
		name:                 "post_like",
		eventType:            postLikeDirtyEventType,
		deltaKey:             postLikeCountDeltaKey,
		dirtyKey:             postLikeCountDirtyKey,
		dirtyAtKey:           postLikeCountDirtyAtKey,
		dirtySinceKey:        postLikeCountDirtySinceKey,
		scheduleKey:          postLikeCountScheduleKey,
		processingBatchKey:   postLikeCountProcessingBatchKey,
		processingSumKey:     postLikeCountProcessingSumKey,
		processingBatchesKey: postLikeCountProcessingBatchesKey,
		dbColumn:             "like_count",
		factCountSQL:         "SELECT COUNT(*) FROM post_like WHERE post_id = ? AND status = 1",
	}
	counterStorageOnce sync.Once
	counterStorageErr  error
)

const claimPostCounterDeltaScript = `
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
redis.call("HSET", KEYS[5], ARGV[5], ARGV[1] .. ":" .. delta)
redis.call("HINCRBY", KEYS[6], ARGV[1], delta)
redis.call("SADD", KEYS[8], ARGV[5])
redis.call("SREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("ZREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[7], ARGV[1])
return delta
`

const clearPostCounterBatchScript = `
local raw = redis.call("HGET", KEYS[1], ARGV[1])
if not raw then
	redis.call("SREM", KEYS[3], ARGV[1])
	return 0
end

local sep = string.find(raw, ":")
if not sep then
	redis.call("HDEL", KEYS[1], ARGV[1])
	redis.call("SREM", KEYS[3], ARGV[1])
	return 0
end

local post_id = string.sub(raw, 1, sep - 1)
local delta = tonumber(string.sub(raw, sep + 1)) or 0
local current = tonumber(redis.call("HINCRBY", KEYS[2], post_id, -delta)) or 0
if current == 0 then
	redis.call("HDEL", KEYS[2], post_id)
end
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("SREM", KEYS[3], ARGV[1])
return post_id
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
		batchTicker := time.NewTicker(postCounterBatchScanInterval)
		reconcileTicker := time.NewTicker(postCounterReconcileInterval)
		defer scanTicker.Stop()
		defer fallbackTicker.Stop()
		defer batchTicker.Stop()
		defer reconcileTicker.Stop()

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
			case <-batchTicker.C:
				if restored, cleaned, err := js.recoverProcessingCounterBatches(ctx, kind, postCounterBatchRecoveryScanSize); err != nil && ctx.Err() == nil {
					js.log.Warnf("recover %s processing counter batches failed: %v", kind.name, err)
				} else if restored > 0 || cleaned > 0 {
					js.log.Debugf("recover %s processing counter batches success, restored=%d, cleaned=%d", kind.name, restored, cleaned)
				}
				if applied, err := js.applyDueCounterBatches(ctx, kind, postCounterBatchScanSize); err != nil && ctx.Err() == nil {
					js.log.Warnf("apply %s counter batches failed: %v", kind.name, err)
				} else if applied > 0 {
					js.log.Debugf("apply %s counter batches success, count=%d", kind.name, applied)
				}
			case <-reconcileTicker.C:
				if fixed, skipped, err := js.reconcilePostCounters(ctx, kind, postCounterReconcileBatchSize); err != nil && ctx.Err() == nil {
					js.log.Warnf("reconcile %s counters failed: %v", kind.name, err)
				} else if fixed > 0 || skipped > 0 {
					js.log.Infof("reconcile %s counters finished, fixed=%d, skipped=%d", kind.name, fixed, skipped)
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
	case "empty":
		return false, nil
	}

	if err := js.insertCounterBatch(ctx, kind, postID, delta, batchID); err != nil {
		_ = js.data.Redis().ZAdd(context.Background(), kind.scheduleKey, redis.Z{
			Score:  float64(time.Now().Add(postLikeCountProcessingRetryDelay).UnixMilli()),
			Member: postIDText,
		}).Err()
		return false, err
	}

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
		kind.processingBatchKey,
		kind.processingSumKey,
		kind.scheduleKey,
		kind.processingBatchesKey,
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
	default:
		delta, err := strconv.ParseInt(value, 10, 64)
		if delta == 0 && err == nil {
			return 0, "empty", nil
		}
		return delta, "claimed", err
	}
}

func (js *JobWorker) insertCounterBatch(ctx context.Context, kind postCounterKind, postID, delta int64, batchID string) error {
	if delta == 0 {
		return nil
	}
	if js.data.DB() == nil {
		return fmt.Errorf("db is not ready")
	}
	if err := ensureCounterStorage(js.data.DB()); err != nil {
		return err
	}

	return js.data.DB().WithContext(ctx).Exec(counterBatchInsertSQL, batchID, kind.name, postID, delta).Error
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

type counterBatchRow struct {
	BatchID string
	PostID  int64
	Delta   int64
}

type counterBatchStateRow struct {
	Status string
	PostID int64
	Delta  int64
}

// recoverProcessingCounterBatches 修复 Redis processing 与 MySQL counter_batch 之间的非事务窗口。
//
// pending -> processing 是 Redis Lua 原子完成的，但随后写 MySQL batch 不是同一个事务。
// 如果进程在这段窗口失败，processing_batch 中还有 batch_id，MySQL 却没有 counter_batch。
// 这个恢复任务会用同一个 batch_id 重新插入 counter_batch，保证后续 applyCounterBatch 仍然幂等。
func (js *JobWorker) recoverProcessingCounterBatches(ctx context.Context, kind postCounterKind, batchSize int64) (int, int, error) {
	if js == nil || js.data == nil || js.data.DB() == nil || js.data.Redis() == nil {
		return 0, 0, nil
	}
	if batchSize <= 0 {
		batchSize = postCounterBatchRecoveryScanSize
	}
	if err := ensureCounterStorage(js.data.DB()); err != nil {
		return 0, 0, err
	}

	batchIDs, err := js.data.Redis().SRandMemberN(ctx, kind.processingBatchesKey, batchSize).Result()
	if err != nil && err != redis.Nil {
		return 0, 0, err
	}

	restored, cleaned := 0, 0
	for _, batchID := range batchIDs {
		if strings.TrimSpace(batchID) == "" {
			continue
		}
		ok, wasRestored, wasCleaned, err := js.recoverProcessingCounterBatch(ctx, kind, batchID)
		if err != nil {
			return restored, cleaned, err
		}
		if !ok {
			continue
		}
		if wasRestored {
			restored++
		}
		if wasCleaned {
			cleaned++
		}
	}
	return restored, cleaned, nil
}

func (js *JobWorker) recoverProcessingCounterBatch(ctx context.Context, kind postCounterKind, batchID string) (bool, bool, bool, error) {
	raw, err := js.data.Redis().HGet(ctx, kind.processingBatchKey, batchID).Result()
	if err == redis.Nil {
		_ = js.data.Redis().SRem(ctx, kind.processingBatchesKey, batchID).Err()
		return false, false, false, nil
	}
	if err != nil {
		return false, false, false, err
	}

	postID, delta, err := parseProcessingCounterBatchValue(raw)
	if err != nil {
		return false, false, false, err
	}

	var state counterBatchStateRow
	err = js.data.DB().WithContext(ctx).
		Table("counter_batch").
		Select("status, post_id, delta").
		Where("batch_id = ? AND counter_type = ?", batchID, kind.name).
		Take(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if err := js.insertCounterBatch(ctx, kind, postID, delta, batchID); err != nil {
			return false, false, false, err
		}
		return true, true, false, nil
	}
	if err != nil {
		return false, false, false, err
	}

	if state.Status == "success" {
		clearedPostID, err := js.clearProcessingPostCounterBatch(ctx, kind, batchID)
		if err != nil {
			return false, false, false, err
		}
		if clearedPostID > 0 {
			_ = js.data.Redis().Del(ctx, buildPostStatsCacheKey(clearedPostID)).Err()
		}
		return true, false, true, nil
	}

	if state.PostID != postID || state.Delta != delta {
		js.log.Warnf("processing %s counter batch differs from db, batch_id=%s, redis_post_id=%d, redis_delta=%d, db_post_id=%d, db_delta=%d",
			kind.name, batchID, postID, delta, state.PostID, state.Delta)
	}
	return true, false, false, nil
}

func (js *JobWorker) applyDueCounterBatches(ctx context.Context, kind postCounterKind, batchSize int) (int, error) {
	if js == nil || js.data == nil || js.data.DB() == nil {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = postCounterBatchScanSize
	}
	if err := ensureCounterStorage(js.data.DB()); err != nil {
		return 0, err
	}

	var rows []counterBatchRow
	if err := js.data.DB().WithContext(ctx).
		Raw(`SELECT batch_id, post_id, delta
FROM counter_batch
WHERE counter_type = ?
  AND status IN ('pending', 'failed')
  AND next_retry_at <= NOW()
ORDER BY id ASC
LIMIT ?`, kind.name, batchSize).
		Scan(&rows).Error; err != nil {
		return 0, err
	}

	applied := 0
	for _, row := range rows {
		if row.BatchID == "" || row.PostID <= 0 || row.Delta == 0 {
			continue
		}
		ok, err := js.applyCounterBatch(ctx, kind, row)
		if err != nil {
			js.markCounterBatchFailed(context.WithoutCancel(ctx), row.BatchID, err)
			continue
		}
		if ok {
			if postID, err := js.clearProcessingPostCounterBatch(ctx, kind, row.BatchID); err != nil {
				js.log.Warnf("clear %s processing batch failed, batch_id=%s, err=%v", kind.name, row.BatchID, err)
			} else if postID > 0 {
				_ = js.data.Redis().Del(ctx, buildPostStatsCacheKey(postID)).Err()
			}
			applied++
		}
	}
	return applied, nil
}

// applyCounterBatch 把一个已进入 counter_batch 的批次落到 post_counter。
//
// status 抢占、post_counter 更新和 success 标记放在同一个 MySQL 事务里，
// 这样失败时不会留下“计数已更新但 batch 仍 pending”的半完成状态。
func (js *JobWorker) applyCounterBatch(ctx context.Context, kind postCounterKind, row counterBatchRow) (bool, error) {
	if err := ensureCounterStorage(js.data.DB()); err != nil {
		return false, err
	}

	claimed := false
	err := js.data.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Exec(`UPDATE counter_batch
SET status = 'processing', updated_at = NOW()
WHERE batch_id = ?
  AND counter_type = ?
  AND status IN ('pending', 'failed')
  AND next_retry_at <= NOW()`, row.BatchID, kind.name)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		claimed = true

		// batch 表控制同一个 batch_id 只进入一次事务；事务内同时改计数和 batch 状态。
		insertLikeCount, insertCommentCount := initialPostCounterValues(kind, row.Delta)
		sql := fmt.Sprintf(`INSERT INTO post_counter (post_id, like_count, comment_count)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE %s = GREATEST(%s + ?, 0)`, kind.dbColumn, kind.dbColumn)
		if err := tx.Exec(sql, row.PostID, insertLikeCount, insertCommentCount, row.Delta).Error; err != nil {
			return err
		}

		return tx.Exec(`UPDATE counter_batch
SET status = 'success', updated_at = NOW(), last_error = ''
WHERE batch_id = ?`, row.BatchID).Error
	})
	return claimed && err == nil, err
}

func (js *JobWorker) markCounterBatchFailed(ctx context.Context, batchID string, cause error) {
	if js == nil || js.data == nil || js.data.DB() == nil || batchID == "" {
		return
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
		if len(msg) > 512 {
			msg = msg[:512]
		}
	}
	nextRetryAt := time.Now().Add(postCounterBatchRetryDelay)
	if err := js.data.DB().WithContext(ctx).Exec(`UPDATE counter_batch
SET status = 'failed',
    retry_count = retry_count + 1,
    next_retry_at = ?,
    last_error = ?,
    updated_at = NOW()
WHERE batch_id = ?`, nextRetryAt, msg, batchID).Error; err != nil {
		js.log.Warnf("mark counter batch failed failed, batch_id=%s, err=%v", batchID, err)
	}
}

// reconcilePostCounters 低频扫描 post_id，并只在没有未落库增量时用事实表覆盖 post_counter。
//
// 它不是实时链路的一部分，而是修复 Redis 极端丢失、历史 bug 或人工修数导致的冗余计数漂移。
func (js *JobWorker) reconcilePostCounters(ctx context.Context, kind postCounterKind, batchSize int) (int, int, error) {
	if js == nil || js.data == nil || js.data.DB() == nil || js.data.Redis() == nil {
		return 0, 0, nil
	}
	if batchSize <= 0 {
		batchSize = postCounterReconcileBatchSize
	}
	if err := ensureCounterStorage(js.data.DB()); err != nil {
		return 0, 0, err
	}

	cursorKey := "counter:" + kind.name + ":reconcile_cursor"
	cursorText, _ := js.data.Redis().Get(ctx, cursorKey).Result()
	cursor, _ := strconv.ParseInt(cursorText, 10, 64)

	var postIDs []int64
	if err := js.data.DB().WithContext(ctx).
		Raw(`SELECT post_id FROM post WHERE post_id > ? ORDER BY post_id ASC LIMIT ?`, cursor, batchSize).
		Scan(&postIDs).Error; err != nil {
		return 0, 0, err
	}
	if len(postIDs) == 0 {
		if cursor > 0 {
			_ = js.data.Redis().Set(ctx, cursorKey, "0", 0).Err()
		}
		return 0, 0, nil
	}

	fixed, skipped := 0, 0
	for _, postID := range postIDs {
		ok, reason, err := js.canReconcilePostCounter(ctx, kind, postID)
		if err != nil {
			return fixed, skipped, err
		}
		if !ok {
			skipped++
			js.recordCounterReconcileLog(ctx, kind, postID, 0, 0, "skipped", reason)
			continue
		}
		changed, err := js.reconcileSinglePostCounter(ctx, kind, postID)
		if err != nil {
			return fixed, skipped, err
		}
		if changed {
			fixed++
		}
	}
	if last := postIDs[len(postIDs)-1]; last > cursor {
		_ = js.data.Redis().Set(ctx, cursorKey, strconv.FormatInt(last, 10), 0).Err()
	}
	return fixed, skipped, nil
}

// canReconcilePostCounter 体现校准的安全条件：
// pending=0、processing_sum=0，并且 counter_batch 没有 pending/failed/processing 批次。
// 任一条件不满足都跳过，避免校准覆盖后又被未完成 batch 重复累加。
func (js *JobWorker) canReconcilePostCounter(ctx context.Context, kind postCounterKind, postID int64) (bool, string, error) {
	postIDText := strconv.FormatInt(postID, 10)
	pipe := js.data.Redis().Pipeline()
	pendingCmd := pipe.HGet(ctx, kind.deltaKey, postIDText)
	processingSumCmd := pipe.HGet(ctx, kind.processingSumKey, postIDText)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return false, "", err
	}
	if redisCounterValue(pendingCmd.Val()) != 0 {
		return false, "has_pending_delta", nil
	}
	if redisCounterValue(processingSumCmd.Val()) != 0 {
		return false, "has_processing_sum", nil
	}

	var unfinished int64
	if err := js.data.DB().WithContext(ctx).
		Raw(`SELECT COUNT(*)
FROM counter_batch
WHERE post_id = ?
  AND counter_type = ?
  AND status IN ('pending', 'failed', 'processing')`, postID, kind.name).
		Scan(&unfinished).Error; err != nil {
		return false, "", err
	}
	if unfinished > 0 {
		return false, "has_unfinished_batch", nil
	}
	return true, "", nil
}

// reconcileSinglePostCounter 使用事实表 count 直接覆盖 post_counter。
//
// 这里不用 +diff，因为校准语义是“事实源覆盖派生表”；重复执行同一个 fact_count 仍然幂等。
func (js *JobWorker) reconcileSinglePostCounter(ctx context.Context, kind postCounterKind, postID int64) (bool, error) {
	var factCount int64
	if err := js.data.DB().WithContext(ctx).Raw(kind.factCountSQL, postID).Scan(&factCount).Error; err != nil {
		return false, err
	}

	var oldCount int64
	query := fmt.Sprintf("SELECT COALESCE(%s, 0) FROM post_counter WHERE post_id = ?", kind.dbColumn)
	err := js.data.DB().WithContext(ctx).Raw(query, postID).Scan(&oldCount).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return false, err
	}
	if oldCount == factCount {
		return false, nil
	}

	insertLikeCount, insertCommentCount := int64(0), int64(0)
	if kind.dbColumn == "like_count" {
		insertLikeCount = factCount
	} else {
		insertCommentCount = factCount
	}
	sql := fmt.Sprintf(`INSERT INTO post_counter (post_id, like_count, comment_count)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE %s = ?`, kind.dbColumn)
	if err := js.data.DB().WithContext(ctx).Exec(sql, postID, insertLikeCount, insertCommentCount, factCount).Error; err != nil {
		return false, err
	}

	_ = js.data.Redis().Del(ctx, buildPostStatsCacheKey(postID)).Err()
	js.recordCounterReconcileLog(ctx, kind, postID, oldCount, factCount, "fixed", "")
	return true, nil
}

func (js *JobWorker) recordCounterReconcileLog(ctx context.Context, kind postCounterKind, postID, oldCount, factCount int64, status, reason string) {
	if js == nil || js.data == nil || js.data.DB() == nil {
		return
	}
	diff := factCount - oldCount
	if err := js.data.DB().WithContext(ctx).Exec(`INSERT INTO counter_reconcile_log(post_id, counter_type, old_count, fact_count, diff, status, reason)
VALUES (?, ?, ?, ?, ?, ?, ?)`, postID, kind.name, oldCount, factCount, diff, status, reason).Error; err != nil {
		js.log.Warnf("record counter reconcile log failed, post_id=%d, counter_type=%s, err=%v", postID, kind.name, err)
	}
}

func ensureCounterStorage(db *gorm.DB) error {
	counterStorageOnce.Do(func() {
		if err := db.Exec(postCounterTableDDL).Error; err != nil {
			counterStorageErr = err
			return
		}
		if err := db.Exec(counterBatchTableDDL).Error; err != nil {
			counterStorageErr = err
			return
		}
		counterStorageErr = db.Exec(counterReconcileLogTableDDL).Error
	})
	return counterStorageErr
}

func redisCounterValue(value interface{}) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	default:
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		return n
	}
}

func parseProcessingCounterBatchValue(raw string) (int64, int64, error) {
	postIDText, deltaText, ok := strings.Cut(raw, ":")
	if !ok {
		return 0, 0, fmt.Errorf("invalid processing counter batch value=%q", raw)
	}
	postID, err := strconv.ParseInt(postIDText, 10, 64)
	if err != nil || postID <= 0 {
		return 0, 0, fmt.Errorf("invalid processing counter batch post_id=%q", postIDText)
	}
	delta, err := strconv.ParseInt(deltaText, 10, 64)
	if err != nil || delta == 0 {
		return 0, 0, fmt.Errorf("invalid processing counter batch delta=%q", deltaText)
	}
	return postID, delta, nil
}

func (js *JobWorker) clearProcessingPostCounterBatch(ctx context.Context, kind postCounterKind, batchID string) (int64, error) {
	raw, err := js.data.Redis().Eval(ctx, clearPostCounterBatchScript, []string{
		kind.processingBatchKey,
		kind.processingSumKey,
		kind.processingBatchesKey,
	}, batchID).Result()
	if err != nil {
		return 0, err
	}
	switch v := raw.(type) {
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, nil
	}
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
