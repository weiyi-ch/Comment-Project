package auth

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	kerrors "github.com/go-kratos/kratos/v2/errors"
)

const studentRole = "student"

type contextKey struct{}

type Principal struct {
	UserID int64
	Role   string
}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(contextKey{}).(Principal)
	return p, ok
}

func RequireStudent(ctx context.Context) (Principal, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok || p.UserID <= 0 {
		return Principal{}, kerrors.Unauthorized("UNAUTHORIZED", "missing student identity")
	}
	if p.Role != studentRole {
		return Principal{}, kerrors.Forbidden("FORBIDDEN", "student role required")
	}
	return p, nil
}

func ParseStudentPrincipal(r *http.Request) (Principal, error) {
	role := strings.TrimSpace(r.Header.Get("x-role"))
	if role == "" {
		role = studentRole
	}
	if role != studentRole {
		return Principal{}, kerrors.Forbidden("FORBIDDEN", "student role required")
	}

	userID, err := parseUserID(r)
	if err != nil || userID <= 0 {
		return Principal{}, kerrors.Unauthorized("UNAUTHORIZED", "missing or invalid student identity")
	}
	return Principal{UserID: userID, Role: role}, nil
}

func parseUserID(r *http.Request) (int64, error) {
	if raw := strings.TrimSpace(r.Header.Get("x-user-id")); raw != "" {
		return strconv.ParseInt(raw, 10, 64)
	}

	authHeader := strings.TrimSpace(r.Header.Get("authorization"))
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	token = strings.TrimSpace(strings.TrimPrefix(token, "bearer "))
	token = strings.TrimPrefix(token, "student:")
	token = strings.TrimPrefix(token, "student-")
	if token == "" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseInt(token, 10, 64)
}
