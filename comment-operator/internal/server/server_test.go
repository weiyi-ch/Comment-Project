package server

import (
	"net/http"
	"testing"

	kerrors "github.com/go-kratos/kratos/v2/errors"
)

func TestAuthErrorStatus(t *testing.T) {
	if got := authErrorStatus(kerrors.Forbidden("FORBIDDEN", "operator role required")); got != http.StatusForbidden {
		t.Fatalf("forbidden error should map to 403, got %d", got)
	}
	if got := authErrorStatus(kerrors.Unauthorized("UNAUTHORIZED", "missing bearer token")); got != http.StatusUnauthorized {
		t.Fatalf("unauthorized error should map to 401, got %d", got)
	}
}
