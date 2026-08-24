package server

import (
	v1 "comment-service/api/comment/v1"
	"comment-service/internal/conf"
	accesslog "comment-service/internal/middleware/accesslog"
	"comment-service/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/middleware/validate"
	"github.com/go-kratos/kratos/v2/transport/http"
)

// NewHTTPServer 创建并注册 HTTP Server。
//
// 返回值是 Kratos HTTP Server，内部已经注册 operator/student/tutor/search 四组 HTTP 接口。
func NewHTTPServer(c *conf.Server, operatorService *service.OperatorService, tutorService *service.TutorService, studentService *service.StudentService, searchService *service.SearchService, logger log.Logger) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			// 中间件顺序代表请求进入业务前的处理顺序：
			// 1. recovery 兜底 panic；
			// 2. tracing 生成/继承 trace_id；
			// 3. accesslog 打印统一访问日志并写回响应头；
			// 4. validate 在进入 service 前校验 proto 参数。
			recovery.Recovery(),
			tracing.Server(),
			accesslog.Server(logger),
			validate.Validator(),
		),
	}
	if c.Http.Network != "" {
		// Network 通常是 tcp，也可以通过配置切换。
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		// Address 决定 HTTP 监听地址，例如 0.0.0.0:8888。
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		// Timeout 是单次 HTTP 请求进入业务后的最大处理时间。
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	// 注册 proto 生成的 HTTP 路由，路由最终会调用对应 service 方法。
	v1.RegisterOperatorServiceHTTPServer(srv, operatorService)
	v1.RegisterStudentServiceHTTPServer(srv, studentService)
	v1.RegisterTutorServiceHTTPServer(srv, tutorService)
	v1.RegisterSearchServiceHTTPServer(srv, searchService)
	return srv
}
