//go:build wireinject
// +build wireinject

package main

import (
	"comment-student/internal/client"
	"comment-student/internal/conf"
	"comment-student/internal/ratelimit"
	"comment-student/internal/server"
	"comment-student/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

func wireApp(confHTTP conf.HTTP, confAuth conf.Auth, confRateLimit conf.RateLimit, confRegistry conf.Registry, confComment conf.CommentService, logger log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, client.ProviderSet, service.ProviderSet, ratelimit.NewLimiter, newApp))
}
