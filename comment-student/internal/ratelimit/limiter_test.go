package ratelimit

import (
	"context"
	"net/http"
	"testing"

	"comment-student/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
)

func TestAPINameNormalizesNumericPathSegments(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/api/student/posts/1001/comments", nil)
	if err != nil {
		t.Fatal(err)
	}

	got := apiName(req)
	want := "post:api/student/posts/{id}/comments"
	if got != want {
		t.Fatalf("apiName() = %q, want %q", got, want)
	}
}

func TestClientIPPrefersForwardedFor(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/api/student/search/posts", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.2")

	got := clientIP(req)
	if got != "203.0.113.7" {
		t.Fatalf("clientIP() = %q, want forwarded client ip", got)
	}
}

func TestDeviceIDFallback(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/api/student/posts/1001", nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := deviceID(req); got != "unknown" {
		t.Fatalf("deviceID() = %q, want unknown", got)
	}

	req.Header.Set("X-Device-ID", "ios-abc")
	if got := deviceID(req); got != "ios-abc" {
		t.Fatalf("deviceID() = %q, want header value", got)
	}
}

func TestDisabledLimiterAllowsWithoutRedis(t *testing.T) {
	limiter, cleanup, err := NewLimiter(conf.RateLimit{Enabled: false}, log.DefaultLogger)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	decision, err := limiter.Allow(context.Background(), []Bucket{
		{Dimension: "api", Key: "rate:api:global:test", Rule: Rule{Rate: 1, Capacity: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Allowed {
		t.Fatal("disabled limiter should allow request")
	}
}
