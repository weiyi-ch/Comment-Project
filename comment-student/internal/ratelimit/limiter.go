package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"time"

	"comment-student/internal/auth"
	"comment-student/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/redis/go-redis/v9"
)

const luaMultiBucket = `
local now = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local tokens = {}
local last = {}
local retry_after = 0

for i = 1, n do
  local rate = tonumber(ARGV[3 + (i - 1) * 2 + 1])
  local capacity = tonumber(ARGV[3 + (i - 1) * 2 + 2])
  local data = redis.call("HMGET", KEYS[i], "tokens", "last_refill_time")
  local current = tonumber(data[1])
  local refill_at = tonumber(data[2])
  if current == nil then
    current = capacity
  end
  if refill_at == nil then
    refill_at = now
  end
  if now > refill_at then
    current = math.min(capacity, current + ((now - refill_at) / 1000) * rate)
    refill_at = now
  end
  tokens[i] = current
  last[i] = refill_at
  if current < requested then
    local wait_ms = math.ceil(((requested - current) / rate) * 1000)
    if retry_after == 0 or wait_ms > retry_after then
      retry_after = wait_ms
    end
    return {0, i, retry_after, current}
  end
end

for i = 1, n do
  local rate = tonumber(ARGV[3 + (i - 1) * 2 + 1])
  local capacity = tonumber(ARGV[3 + (i - 1) * 2 + 2])
  local next_tokens = tokens[i] - requested
  local ttl = math.ceil((capacity / rate) * 2000)
  redis.call("HSET", KEYS[i], "tokens", next_tokens, "last_refill_time", last[i])
  redis.call("PEXPIRE", KEYS[i], ttl)
end

return {1, 0, 0, 0}
`

type Limiter struct {
	enabled     bool
	client      *redis.Client
	log         *log.Helper
	rules       map[string]Rule
	defaultRule Rule
}

type Rule struct {
	Rate     float64
	Capacity float64
}

type Bucket struct {
	Dimension string
	Key       string
	Rule      Rule
}

type Decision struct {
	Allowed       bool
	Dimension     string
	RetryAfter    time.Duration
	RemainingHint float64
}

func NewLimiter(cfg conf.RateLimit, logger log.Logger) (*Limiter, func(), error) {
	if !cfg.Enabled {
		return &Limiter{enabled: false}, func() {}, nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})
	limiter := &Limiter{
		enabled: true,
		client:  client,
		log:     log.NewHelper(log.With(logger, "module", "ratelimit")),
		rules: map[string]Rule{
			"user":   {Rate: cfg.User.Rate, Capacity: cfg.User.Capacity},
			"ip":     {Rate: cfg.IP.Rate, Capacity: cfg.IP.Capacity},
			"device": {Rate: cfg.Device.Rate, Capacity: cfg.Device.Capacity},
			"api":    {Rate: cfg.API.Rate, Capacity: cfg.API.Capacity},
		},
		defaultRule: Rule{Rate: cfg.API.Rate, Capacity: cfg.API.Capacity},
	}
	return limiter, func() {
		_ = client.Close()
	}, nil
}

func (l *Limiter) AuthFilter() http.FilterFunc {
	return l.filter(false)
}

func (l *Limiter) StudentFilter() http.FilterFunc {
	return l.filter(true)
}

func (l *Limiter) filter(requireUser bool) http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			if l == nil || !l.enabled {
				next.ServeHTTP(w, r)
				return
			}
			buckets := l.bucketsForRequest(r, requireUser)
			decision, err := l.Allow(r.Context(), buckets)
			if err != nil {
				l.log.Warnf("rate limit check failed: %v", err)
				next.ServeHTTP(w, r)
				return
			}
			if !decision.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(int(decision.RetryAfter.Seconds()+0.999)))
				w.Header().Set("X-RateLimit-Limited-Dimension", decision.Dimension)
				stdhttp.Error(w, "too many requests", stdhttp.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (l *Limiter) Allow(ctx context.Context, buckets []Bucket) (Decision, error) {
	if l == nil || !l.enabled || len(buckets) == 0 {
		return Decision{Allowed: true}, nil
	}
	keys := make([]string, 0, len(buckets))
	args := make([]any, 0, 3+len(buckets)*2)
	args = append(args, time.Now().UnixMilli(), 1, len(buckets))
	for _, b := range buckets {
		if b.Rule.Rate <= 0 || b.Rule.Capacity <= 0 {
			return Decision{}, fmt.Errorf("invalid rate limit rule for %s", b.Dimension)
		}
		keys = append(keys, b.Key)
		args = append(args, b.Rule.Rate, b.Rule.Capacity)
	}
	res, err := l.client.Eval(ctx, luaMultiBucket, keys, args...).Result()
	if err != nil {
		return Decision{}, err
	}
	values, err := redisIntSlice(res)
	if err != nil {
		return Decision{}, err
	}
	if len(values) < 4 || values[0] == 1 {
		return Decision{Allowed: true}, nil
	}
	blockedIndex := int(values[1]) - 1
	dimension := "unknown"
	if blockedIndex >= 0 && blockedIndex < len(buckets) {
		dimension = buckets[blockedIndex].Dimension
	}
	retryAfter := time.Duration(values[2]) * time.Millisecond
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	return Decision{
		Allowed:       false,
		Dimension:     dimension,
		RetryAfter:    retryAfter,
		RemainingHint: float64(values[3]),
	}, nil
}

func (l *Limiter) bucketsForRequest(r *stdhttp.Request, includeUser bool) []Bucket {
	api := apiName(r)
	buckets := make([]Bucket, 0, 4)
	if includeUser {
		if p, ok := auth.PrincipalFromContext(r.Context()); ok && p.UserID > 0 {
			buckets = append(buckets, l.bucket("user", strconv.FormatInt(p.UserID, 10), api))
		}
	}
	buckets = append(buckets,
		l.bucket("ip", clientIP(r), api),
		l.bucket("device", deviceID(r), api),
		l.bucket("api", "global", api),
	)
	return buckets
}

func (l *Limiter) bucket(dimension, id, api string) Bucket {
	rule := l.rules[dimension]
	if rule.Rate <= 0 || rule.Capacity <= 0 {
		rule = l.defaultRule
	}
	return Bucket{
		Dimension: dimension,
		Key:       fmt.Sprintf("rate:%s:%s:%s", dimension, sanitizeKeyPart(id), sanitizeKeyPart(api)),
		Rule:      rule,
	}
}

func apiName(r *stdhttp.Request) string {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i, part := range parts {
		if _, err := strconv.ParseInt(part, 10, 64); err == nil {
			parts[i] = "{id}"
		}
	}
	return strings.ToLower(r.Method + ":" + strings.Join(parts, "/"))
}

func clientIP(r *stdhttp.Request) string {
	for _, header := range []string{"X-Forwarded-For", "X-Real-IP"} {
		value := strings.TrimSpace(r.Header.Get(header))
		if value == "" {
			continue
		}
		ip := strings.TrimSpace(strings.Split(value, ",")[0])
		if parsed := net.ParseIP(ip); parsed != nil {
			return parsed.String()
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	if parsed := net.ParseIP(r.RemoteAddr); parsed != nil {
		return parsed.String()
	}
	return "unknown"
}

func deviceID(r *stdhttp.Request) string {
	for _, header := range []string{"X-Device-ID", "X-Device-Id"} {
		value := strings.TrimSpace(r.Header.Get(header))
		if value != "" {
			return value
		}
	}
	return "unknown"
}

func sanitizeKeyPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer(" ", "_", "\t", "_", "\n", "_", "\r", "_", ":", "_")
	return replacer.Replace(value)
}

func redisIntSlice(v any) ([]int64, error) {
	items, ok := v.([]any)
	if !ok {
		return nil, errors.New("unexpected redis lua response")
	}
	out := make([]int64, 0, len(items))
	for _, item := range items {
		switch val := item.(type) {
		case int64:
			out = append(out, val)
		case string:
			parsed, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, err
			}
			out = append(out, parsed)
		default:
			return nil, fmt.Errorf("unexpected redis lua item type %T", item)
		}
	}
	return out, nil
}
