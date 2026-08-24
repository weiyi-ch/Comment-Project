//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"comment-task/internal/biz"
	"comment-task/internal/conf"
	"comment-task/internal/data"
	"comment-task/internal/server"
	"comment-task/internal/service"
	"comment-task/internal/task"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.Kafka, *conf.Elasticsearch, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, task.ProviderSet, newApp))
}
