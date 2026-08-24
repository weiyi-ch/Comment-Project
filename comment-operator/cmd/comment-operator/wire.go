//go:build wireinject
// +build wireinject

package main

import (
	"comment-operator/internal/client"
	"comment-operator/internal/conf"
	"comment-operator/internal/server"
	"comment-operator/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

func wireApp(confHTTP conf.HTTP, confRegistry conf.Registry, confComment conf.CommentService, logger log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, client.ProviderSet, service.ProviderSet, newApp))
}
