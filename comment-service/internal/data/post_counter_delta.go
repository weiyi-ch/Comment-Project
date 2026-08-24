package data

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func redisInt64(raw interface{}) int64 {
	switch v := raw.(type) {
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
		return 0
	}
}

func (d *Data) counterDelta(ctx context.Context, pendingKey, processingKey string, postID int64, label string) int64 {
	if d.cache == nil {
		return 0
	}

	postIDText := strconv.FormatInt(postID, 10)
	pipe := d.cache.Pipeline()
	pendingCmd := pipe.HGet(ctx, pendingKey, postIDText)
	processingCmd := pipe.HGet(ctx, processingKey, postIDText)
	if _, err := pipe.Exec(ctx); err == nil || err == redis.Nil {
		return redisCounterValue(pendingCmd.Val()) + redisCounterValue(processingCmd.Val())
	}

	pending, pendingErr := d.cache.HGet(ctx, pendingKey, postIDText).Int64()
	if pendingErr != nil && pendingErr != redis.Nil {
		d.log.WithContext(ctx).Warnf("get pending %s count delta failed, post_id=%d, err=%v", label, postID, pendingErr)
	}
	processing, processingErr := d.cache.HGet(ctx, processingKey, postIDText).Int64()
	if processingErr != nil && processingErr != redis.Nil {
		d.log.WithContext(ctx).Warnf("get processing %s count delta failed, post_id=%d, err=%v", label, postID, processingErr)
	}
	return pending + processing
}

func redisCounterValue(value interface{}) int64 {
	switch v := value.(type) {
	case nil:
		return 0
	case int64:
		return v
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
