package server

import (
	v1 "comment-service/api/comment/v1"
	"comment-service/internal/conf"
	accesslog "comment-service/internal/middleware/accesslog"
	"comment-service/internal/mtls"
	"comment-service/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/middleware/validate"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

// NewGRPCServer 创建并注册 gRPC Server。
//
// 返回值是 Kratos gRPC Server，内部已经注册 operator/student/tutor/search 四组 RPC 接口。
func NewGRPCServer(c *conf.Server, operatorService *service.OperatorService, tutorService *service.TutorService, studentService *service.StudentService, searchService *service.SearchService, logger log.Logger) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			// gRPC 和 HTTP 使用同一套入口治理能力，保证压测和排障时日志字段一致。
			recovery.Recovery(),
			tracing.Server(),
			accesslog.Server(logger),
			validate.Validator(),
		),
	}
	if c.Grpc.Network != "" {
		// Network 通常是 tcp，也可以通过配置切换。
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		// Address 决定 gRPC 监听地址，例如 0.0.0.0:9000。
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		// Timeout 是单次 gRPC 请求进入业务后的最大处理时间。
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	if tlsCfg := c.GetGrpc().GetTls(); tlsCfg.GetEnabled() {
		tlsConf, err := mtls.ServerConfig(tlsCfg.GetCaFile(), tlsCfg.GetCertFile(), tlsCfg.GetKeyFile(), tlsCfg.GetAllowedClients())
		if err != nil {
			panic(err)
		}
		opts = append(opts, grpc.TLSConfig(tlsConf))
	}
	srv := grpc.NewServer(opts...)
	// 注册 proto 生成的 gRPC 服务描述，最终路由到对应 service 方法。
	v1.RegisterOperatorServiceServer(srv, operatorService)
	v1.RegisterStudentServiceServer(srv, studentService)
	v1.RegisterTutorServiceServer(srv, tutorService)
	v1.RegisterSearchServiceServer(srv, searchService)
	return srv
}
