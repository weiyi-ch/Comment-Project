//go:build wireinject
// +build wireinject

package main

import (
	"comment-tutor/internal/client"
	"comment-tutor/internal/conf"
	"comment-tutor/internal/server"
	"comment-tutor/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

func wireApp(confHTTP conf.HTTP, confAuth conf.Auth, confRegistry conf.Registry, confComment conf.CommentService, logger log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, client.ProviderSet, service.ProviderSet, newApp))
}
