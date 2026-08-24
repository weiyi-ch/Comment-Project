package auth

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTokenLifecycleAndPasswordHash(t *testing.T) {
	storeFile := filepath.Join(t.TempDir(), "auth.json")
	svc, err := NewService(Config{
		Role:            tutorRole,
		SigningSecret:   "test-secret",
		StoreFile:       storeFile,
		AccessTokenTTL:  time.Minute,
		RefreshTokenTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := svc.Register("alice", "password123")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(storeFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "password123") {
		t.Fatal("password was stored in plaintext")
	}
	if !strings.Contains(string(data), "$2a$") && !strings.Contains(string(data), "$2b$") {
		t.Fatal("bcrypt password hash was not stored")
	}

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+reply.AccessToken)
	p, err := svc.ParsePrincipal(req)
	if err != nil {
		t.Fatal(err)
	}
	if p.UserID != reply.UserID || p.Role != tutorRole || p.TokenID == "" {
		t.Fatalf("unexpected principal: %+v", p)
	}

	rotated, err := svc.Refresh(reply.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Refresh(reply.RefreshToken); err == nil {
		t.Fatal("old refresh token should be revoked after rotation")
	}
	if err := svc.Logout(rotated.RefreshToken); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Refresh(rotated.RefreshToken); err == nil {
		t.Fatal("logged out refresh token should be revoked")
	}
}

func TestParsePrincipalRejectsUserIDHeaderOnly(t *testing.T) {
	svc, err := NewService(Config{
		Role:          tutorRole,
		SigningSecret: "test-secret",
		StoreFile:     filepath.Join(t.TempDir(), "auth.json"),
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
