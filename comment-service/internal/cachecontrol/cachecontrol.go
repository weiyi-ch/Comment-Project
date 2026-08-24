package cachecontrol

import (
	"context"
	"os"
	"strings"

	"github.com/go-kratos/kratos/v2/transport"
)

// 缓存绕过开关的请求头和环境变量名。
//
// 这些开关只影响读写缓存的 helper，不影响写路径的缓存删除。
const (
	headerCacheMode   = "x-cache-mode"
	headerCacheBypass = "x-cache-bypass"
	envCacheMode      = "CACHE_MODE"
	envCacheBypass    = "CACHE_BYPASS"
)

// Bypass 判断当前请求是否需要跳过 Redis 缓存读写。
//
// 这个开关用于压测和排障：
// 1. 环境变量 CACHE_BYPASS/CACHE_MODE 可以让整个进程绕过缓存；
// 2. 请求头 x-cache-bypass/x-cache-mode 可以让单次请求绕过缓存；
// 3. 缓存删除不走该 helper，写路径仍然应该正常失效旧缓存。
func Bypass(ctx context.Context) bool {
	// 环境变量优先，适合本地压测时临时关闭缓存。
	if isBypassValue(os.Getenv(envCacheBypass)) || isBypassValue(os.Getenv(envCacheMode)) {
		return true
	}

	// Kratos transport 中保存了 HTTP/gRPC 请求头，Postman/k6 可通过 header 控制单次请求。
	if tr, ok := transport.FromServerContext(ctx); ok {
		headers := tr.RequestHeader()
		return isBypassValue(headers.Get(headerCacheMode)) || isBypassValue(headers.Get(headerCacheBypass))
	}

	return false
}

// isBypassValue 判断字符串是否表达“跳过缓存”的语义。
//
// 返回 true 的值故意兼容多种写法，方便 Postman、k6、环境变量直接使用。
func isBypassValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on", "bypass", "no-cache", "nocache", "skip", "off", "disable", "disabled":
		return true
	default:
		return false
	}
}
