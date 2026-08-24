package server

import (
	"comment-service/internal/conf"

	consul "github.com/go-kratos/kratos/contrib/registry/consul/v2"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

// ProviderSet 声明 server 层可被 wire 注入的构造函数集合。
var ProviderSet = wire.NewSet(NewRegistrar, NewGRPCServer, NewHTTPServer)

// NewRegistrar 创建 Consul 服务注册器。
//
// Kratos App 启动时会通过该注册器把当前服务注册到 Consul，方便其他服务发现。
func NewRegistrar(conf *conf.Registry) registry.Registrar {
	// api.DefaultConfig 返回 Consul 官方客户端默认配置。
	c := api.DefaultConfig()
	// 配置文件中的 address/scheme 决定注册中心访问地址。
	c.Address = conf.Consul.Address
	c.Scheme = conf.Consul.Scheme
	// api.NewClient 返回 Consul 客户端，失败通常代表配置非法。
	client, err := api.NewClient(c)
	if err != nil {
		panic(err)
	}
	// consul.New 把官方客户端包装成 Kratos registry.Registrar。
	reg := consul.New(client)
	return reg
}
