package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	commentv1 "comment-tutor/api/comment/v1"

	"google.golang.org/grpc"
)

type fakeAuthClient struct{}

func (fakeAuthClient) Register(context.Context, *commentv1.RegisterRequest, ...grpc.CallOption) (*commentv1.TokenReply, error) {
	return nil, nil
}

func (fakeAuthClient) Login(context.Context, *commentv1.LoginRequest, ...grpc.CallOption) (*commentv1.TokenReply, error) {
	return nil, nil
}

func (fakeAuthClient) Refresh(context.Context, *commentv1.RefreshRequest, ...grpc.CallOption) (*commentv1.TokenReply, error) {
	return nil, nil
}

func (fakeAuthClient) Logout(context.Context, *commentv1.LogoutRequest, ...grpc.CallOption) (*commentv1.LogoutReply, error) {
	return nil, nil
}

func TestParsePrincipalValidatesAccessToken(t *testing.T) {
	svc, err := NewService(Config{
		Role:          tutorRole,
		SigningSecret: "test-secret",
		AuthClient:    fakeAuthClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	token := signTestToken(t, "test-secret", claims{
		TokenID:   "token-1",
		UserID:    123,
		Role:      tutorRole,
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	p, err := svc.ParsePrincipal(req)
	if err != nil {
		t.Fatal(err)
	}
	if p.UserID != 123 || p.Role != tutorRole || p.TokenID != "token-1" {
		t.Fatalf("unexpected principal: %+v", p)
	}
}

func TestParsePrincipalRejectsUserIDHeaderOnly(t *testing.T) {
	svc, err := NewService(Config{
		Role:          tutorRole,
		SigningSecret: "test-secret",
		AuthClient:    fakeAuthClient{},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("x-user-id", "123")
	if _, err := svc.ParsePrincipal(req); err == nil {
		t.Fatal("x-user-id without bearer token should be rejected")
	}
}

func signTestToken(t *testing.T, secret string, c claims) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyData, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	body := base64.RawURLEncoding.EncodeToString(bodyData)
	input := header + "." + body
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
