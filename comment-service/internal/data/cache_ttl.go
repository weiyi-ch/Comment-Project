package data

import (
	"math/rand"
	"time"
)

const cacheTTLJitterDivisor = 10

// cacheTTLWithJitter 给缓存 TTL 增加 +/-10% 的随机抖动。
//
// 没有抖动时，压测或流量高峰批量创建的 key 可能在同一秒集中过期，
// 从而把大量请求同时打回 MySQL/ES。
//
// 返回值：
// 1. ttl <= 0：保持原值；
// 2. 正常 ttl：返回加过随机偏移后的 ttl。
func cacheTTLWithJitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}

	// jitter 是最大抖动幅度，这里设置为原始 TTL 的 10%。
	jitter := ttl / cacheTTLJitterDivisor
	if jitter <= 0 {
		return ttl
	}

	// delta 落在 [-jitter, +jitter] 区间，让不同 key 的过期时间错开。
	delta := time.Duration(rand.Int63n(int64(jitter)*2+1)) - jitter
	jittered := ttl + delta
	if jittered <= 0 {
		return ttl
	}

	return jittered
}
