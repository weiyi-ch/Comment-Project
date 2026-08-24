package data

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"comment-service/dal/model"
	"comment-service/internal/cachecontrol"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

const (
	postCoreCachePrefix  = "mysql:post:core:"
	postStatsCachePrefix = "mysql:post:stats:"
)

// postStatsCache 只保存变化频率较高的帖子统计字段。
// 帖子主体缓存不再因为评论数或点赞数变化而整体失效。
type postStatsCache struct {
	LikeCount    int32 `json:"like_count"`
	CommentCount int32 `json:"comment_count"`
}

type queryPostFunc func(context.Context, int64) (*model.Post, error)
type queryPostStatsFunc func(context.Context, int64) (*postStatsCache, error)

func buildPostCoreCacheKey(postID int64) string {
	return postCoreCachePrefix + strconv.FormatInt(postID, 10)
}

func buildPostStatsCacheKey(postID int64) string {
	return postStatsCachePrefix + strconv.FormatInt(postID, 10)
}

// loadPostWithSplitCache 使用“帖子主体 + 统计字段”两级对象缓存读取帖子。
//
// core miss 时查询完整帖子并同时回填 core/stats；
// core hit、stats miss 时只查询 like_count/comment_count，避免评论数变化导致整篇帖子回源。
func loadPostWithSplitCache(
	ctx context.Context,
	cache *redis.Client,
	group *singleflight.Group,
	postID int64,
	nullValue string,
	coreTTL, statsTTL, nullTTL time.Duration,
	queryPost queryPostFunc,
	queryStats queryPostStatsFunc,
) (*model.Post, error) {
	coreKey := buildPostCoreCacheKey(postID)
	statsKey := buildPostStatsCacheKey(postID)

	core, coreHit, err := readPostCoreCache(ctx, cache, coreKey, nullValue)
	if err == nil && coreHit {
		if core == nil {
			return nil, gorm.ErrRecordNotFound
		}
		return completePostStats(ctx, cache, group, core, statsKey, statsTTL, queryStats)
	}

	val, err, _ := group.Do(coreKey, func() (interface{}, error) {
		// 等待 singleflight 时，其他请求或其他链路可能已经回填了主体缓存。
		core, hit, cacheErr := readPostCoreCache(ctx, cache, coreKey, nullValue)
		if cacheErr == nil && hit {
			if core == nil {
				return nil, gorm.ErrRecordNotFound
			}
			return completePostStats(ctx, cache, group, core, statsKey, statsTTL, queryStats)
		}

		post, queryErr := queryPost(ctx, postID)
		if queryErr != nil {
			if errors.Is(queryErr, gorm.ErrRecordNotFound) {
				writePostNullCache(ctx, cache, coreKey, nullValue, nullTTL)
			}
			return nil, queryErr
		}

		writePostSplitCache(ctx, cache, post, coreTTL, statsTTL)
		return post, nil
	})
	if err != nil {
		return nil, err
	}

	post, ok := val.(*model.Post)
	if !ok || post == nil {
		return nil, gorm.ErrRecordNotFound
	}
	return post, nil
}

func completePostStats(
	ctx context.Context,
	cache *redis.Client,
	group *singleflight.Group,
	core *model.Post,
	statsKey string,
	statsTTL time.Duration,
	queryStats queryPostStatsFunc,
) (*model.Post, error) {
	stats, hit, err := readPostStatsCache(ctx, cache, statsKey)
	if err == nil && hit {
		return mergePostStats(core, stats), nil
	}

	val, err, _ := group.Do(statsKey, func() (interface{}, error) {
		stats, hit, cacheErr := readPostStatsCache(ctx, cache, statsKey)
		if cacheErr == nil && hit {
			return stats, nil
		}

		stats, queryErr := queryStats(ctx, core.PostID)
		if queryErr != nil {
			return nil, queryErr
		}
		writePostStatsCache(ctx, cache, statsKey, stats, statsTTL)
		return stats, nil
	})
	if err != nil {
		return nil, err
	}

	stats, ok := val.(*postStatsCache)
	if !ok || stats == nil {
		return nil, gorm.ErrRecordNotFound
	}
	return mergePostStats(core, stats), nil
}

func readPostCoreCache(ctx context.Context, cache *redis.Client, key, nullValue string) (*model.Post, bool, error) {
	if cache == nil || cachecontrol.Bypass(ctx) {
		return nil, false, nil
	}

	data, err := cache.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if string(data) == nullValue {
		return nil, true, nil
	}

	var post model.Post
	if err := json.Unmarshal(data, &post); err != nil {
		_ = cache.Del(ctx, key).Err()
		return nil, false, err
	}
	return &post, true, nil
}

func readPostStatsCache(ctx context.Context, cache *redis.Client, key string) (*postStatsCache, bool, error) {
	if cache == nil || cachecontrol.Bypass(ctx) {
		return nil, false, nil
	}

	data, err := cache.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	var stats postStatsCache
	if err := json.Unmarshal(data, &stats); err != nil {
		_ = cache.Del(ctx, key).Err()
		return nil, false, err
	}
	return &stats, true, nil
}

func writePostSplitCache(ctx context.Context, cache *redis.Client, post *model.Post, coreTTL, statsTTL time.Duration) {
	if cache == nil || post == nil || cachecontrol.Bypass(ctx) {
		return
	}

	core := *post
	core.LikeCount = 0
	core.CommentCount = 0
	stats := &postStatsCache{
		LikeCount:    post.LikeCount,
		CommentCount: post.CommentCount,
	}

	coreData, coreErr := json.Marshal(&core)
	statsData, statsErr := json.Marshal(stats)
	if coreErr != nil || statsErr != nil {
		return
	}

	pipe := cache.TxPipeline()
	pipe.Set(ctx, buildPostCoreCacheKey(post.PostID), coreData, cacheTTLWithJitter(coreTTL))
	pipe.Set(ctx, buildPostStatsCacheKey(post.PostID), statsData, cacheTTLWithJitter(statsTTL))
	_, _ = pipe.Exec(ctx)
}

func writePostStatsCache(ctx context.Context, cache *redis.Client, key string, stats *postStatsCache, ttl time.Duration) {
	if cache == nil || stats == nil || cachecontrol.Bypass(ctx) {
		return
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return
	}
	_ = cache.Set(ctx, key, data, cacheTTLWithJitter(ttl)).Err()
}

func writePostCoreCaches(ctx context.Context, cache *redis.Client, posts []*model.Post, ttl time.Duration) {
	if cache == nil || len(posts) == 0 || cachecontrol.Bypass(ctx) {
		return
	}

	pipe := cache.TxPipeline()
	for _, post := range posts {
		if post == nil || post.PostID <= 0 {
			continue
		}
		core := *post
		core.LikeCount = 0
		core.CommentCount = 0
		data, err := json.Marshal(&core)
		if err != nil {
			continue
		}
		pipe.Set(ctx, buildPostCoreCacheKey(post.PostID), data, cacheTTLWithJitter(ttl))
	}
	_, _ = pipe.Exec(ctx)
}

func writePostNullCache(ctx context.Context, cache *redis.Client, key, nullValue string, ttl time.Duration) {
	if cache == nil || cachecontrol.Bypass(ctx) {
		return
	}
	_ = cache.Set(ctx, key, []byte(nullValue), cacheTTLWithJitter(ttl)).Err()
}

func mergePostStats(core *model.Post, stats *postStatsCache) *model.Post {
	if core == nil || stats == nil {
		return core
	}
	post := *core
	post.LikeCount = stats.LikeCount
	post.CommentCount = stats.CommentCount
	return &post
}
