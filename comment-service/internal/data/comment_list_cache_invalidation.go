package data

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// deleteCommentListCaches 删除评论相关的“列表 ID 缓存”。
//
// 评论列表缓存是查询结果缓存，value 只保存 comment_id 列表和 total。
// 新增、删除、审核会影响排序、分页和 total，因此写操作不应该手动修改列表内容，
// 而是删除相关列表缓存，让下一次读请求从 MySQL 重新构建。
func deleteCommentListCaches(ctx context.Context, cache *redis.Client, postID, studentID int64, auditStatuses ...int32) error {
	if cache == nil {
		return nil
	}

	prefixes := make([]string, 0, 5+len(auditStatuses))
	if postID > 0 {
		// 一个帖子下的评论变化会影响学生端、助教端、运营端的帖子评论分页列表。
		prefixes = append(prefixes,
			buildStudentPostCommentListCacheKeyPrefix(postID),
			buildTutorCommentListCacheKeyPrefix(postID),
			buildOperatorPostCommentListCacheKeyPrefix(postID),
		)
	}
	if studentID > 0 {
		// 学生自己的评论列表也会因为新增/删除评论发生变化。
		prefixes = append(prefixes, buildStudentMyCommentListCacheKeyPrefix(studentID))
	}
	for _, status := range auditStatuses {
		// 审核状态列表按 audit_status 做前缀隔离，审核动作需要删除相关状态列表。
		prefixes = append(prefixes, buildOperatorAuditCommentListCacheKeyPrefix(status))
	}

	return deleteCacheByPrefixes(ctx, cache, prefixes...)
}

// deleteCacheByPrefixes 按前缀扫描并删除 Redis key。
//
// 返回值：
// 1. nil：所有前缀删除完成；
// 2. error：SCAN 或 DEL 失败，调用方会记录日志但不回滚 MySQL 写入。
func deleteCacheByPrefixes(ctx context.Context, cache *redis.Client, prefixes ...string) error {
	if cache == nil {
		return nil
	}

	for _, prefix := range prefixes {
		if prefix == "" {
			continue
		}

		var cursor uint64
		for {
			// SCAN 是游标式遍历，避免 KEYS 在生产环境阻塞 Redis。
			keys, next, err := cache.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				// DEL 返回删除数量，但当前只关心是否删除失败。
				if err := cache.Del(ctx, keys...).Err(); err != nil {
					return err
				}
			}

			// next 为 0 表示当前前缀扫描完成。
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}

	return nil
}
