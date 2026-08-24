//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"comment-service/internal/biz"
	"comment-service/internal/conf"
	"comment-service/internal/data"
	"comment-service/internal/server"
	"comment-service/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp 是 wire 的依赖注入声明入口。
//
// 该文件只在执行 wire 生成代码时参与编译；真正运行时使用 wire_gen.go。
func wireApp(*conf.Server, *conf.Registry, *conf.Elasticsearch, *conf.Data, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, newApp))
}
