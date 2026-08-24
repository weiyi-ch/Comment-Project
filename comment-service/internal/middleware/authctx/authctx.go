package authctx

import (
	"context"
	"strconv"

	serviceauth "comment-service/internal/authctx"

	"github.com/go-kratos/kratos/v2/middleware"
	"google.golang.org/grpc/metadata"
)

func Server() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return handler(ctx, req)
			}
			userID, err := strconv.ParseInt(first(md.Get("x-user-id")), 10, 64)
			if err != nil || userID <= 0 {
				return handler(ctx, req)
			}
			p := serviceauth.Principal{
				UserID:  userID,
				Role:    first(md.Get("x-role")),
				TokenID: first(md.Get("x-token-id")),
			}
			return handler(serviceauth.WithPrincipal(ctx, p), req)
		}
	}
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
