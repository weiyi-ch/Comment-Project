//go:build wireinject
// +build wireinject

package main

import (
	"student-bff/internal/client"
	"student-bff/internal/conf"
	"student-bff/internal/server"
	"student-bff/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

func wireApp(confHTTP conf.HTTP, confRegistry conf.Registry, confComment conf.CommentService, logger log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, client.ProviderSet, service.ProviderSet, newApp))
}
