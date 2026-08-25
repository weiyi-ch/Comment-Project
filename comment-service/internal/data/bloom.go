package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	globalBloomPostKey    = "bf:post"
	globalBloomCommentKey = "bf:comment"

	bloomPostReadyKey    = "bf:post:ready"
	bloomCommentReadyKey = "bf:comment:ready"

	bloomWarmupBatchSize = 1000
	bloomErrorRate       = "0.001"
	bloomPostCapacity    = "200000"
	bloomCommentCapacity = "8000000"
)

// startBloomWarmup 在服务启动后异步重建 Bloom。
//
// 重建完成前 bloomExists 会旁路 Bloom，避免 Redis 空 Bloom 把 MySQL 中真实存在的历史 ID 误判为不存在。
func (d *Data) startBloomWarmup(ctx context.Context) {
	if d == nil || d.db == nil || d.cache == nil {
		return
	}

	go func() {
		if err := d.rebuildBloomFilters(ctx); err != nil && ctx.Err() == nil {
			d.log.WithContext(ctx).Warnf("rebuild bloom filters failed: %v", err)
		}
	}()
}

func (d *Data) rebuildBloomFilters(ctx context.Context) error {
	if err := d.rebuildBloomFilter(ctx, globalBloomPostKey, bloomPostReadyKey, bloomPostCapacity, "post", "post_id"); err != nil {
		return err
	}
	return d.rebuildBloomFilter(ctx, globalBloomCommentKey, bloomCommentReadyKey, bloomCommentCapacity, "study_comment", "comment_id")
}

func (d *Data) rebuildBloomFilter(ctx context.Context, bloomKey, readyKey, capacity, table, idColumn string) error {
	cache := d.cache
	if err := cache.Del(ctx, bloomKey, readyKey).Err(); err != nil {
		return err
	}
	if err := reserveBloomFilter(ctx, cache, bloomKey, bloomErrorRate, capacity); err != nil {
		return err
	}

	var cursor int64
	for {
		var ids []int64
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?", idColumn, table, idColumn, idColumn)
		if err := d.db.WithContext(ctx).Raw(sql, cursor, bloomWarmupBatchSize).Scan(&ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if err := bloomMAdd(ctx, cache, bloomKey, ids); err != nil {
			return err
		}
		cursor = ids[len(ids)-1]
	}

	return cache.Set(ctx, readyKey, strconv.FormatInt(time.Now().Unix(), 10), 0).Err()
}

func reserveBloomFilter(ctx context.Context, cache *redis.Client, bloomKey, errorRate, capacity string) error {
	err := cache.Do(ctx, "BF.RESERVE", bloomKey, errorRate, capacity).Err()
	if err == nil || isBloomKeyExistsError(err) {
		return nil
	}
	return err
}

func bloomMAdd(ctx context.Context, cache *redis.Client, bloomKey string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(ids)+2)
	args = append(args, "BF.MADD", bloomKey)
	for _, id := range ids {
		if id > 0 {
			args = append(args, strconv.FormatInt(id, 10))
		}
	}
	if len(args) == 2 {
		return nil
	}
	return cache.Do(ctx, args...).Err()
}

func isBloomReady(ctx context.Context, cache *redis.Client, readyKey string) bool {
	if cache == nil {
		return false
	}
	ok, err := cache.Exists(ctx, readyKey).Result()
	return err == nil && ok > 0
}

func bloomReadyKey(bloomKey string) string {
	switch bloomKey {
	case "bf:post":
		return bloomPostReadyKey
	case "bf:comment":
		return bloomCommentReadyKey
	default:
		return bloomKey + ":ready"
	}
}

func isBloomKeyExistsError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "exists") || strings.Contains(msg, "busykey")
}
