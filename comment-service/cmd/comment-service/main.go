package main

import (
	"context"
	"flag"
	"os"
	"time"

	"comment-service/internal/conf"
	"comment-service/internal/observability"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/go-kratos/kratos/v2/transport/http"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string = "comment-service"
	// Version is the version of the compiled software.
	Version string = "v1"
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

// init 注册命令行参数。
//
// 当前只注册 -conf，用于指定 Kratos 配置目录或配置文件路径。
func init() {
	// -conf 支持传入目录或文件路径，Kratos file source 会从该位置加载配置。
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

// newApp 组装 Kratos 应用实例。
//
// wire 会先构造 logger、注册中心、gRPC Server、HTTP Server，
// 最后把这些依赖传入 newApp，形成真正可以 Run 的应用。
func newApp(logger log.Logger, r registry.Registrar, gs *grpc.Server, hs *http.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			gs,
			hs,
		),
		kratos.Registrar(r),
	)
}

// main 是 comment-service 的进程入口。
//
// 启动流程：
// 1. 解析配置路径；
// 2. 初始化带 trace 字段的 logger；
// 3. 加载配置到 conf.Bootstrap；
// 4. 通过 wireApp 完成依赖注入；
// 5. 启动 Kratos 应用并阻塞等待退出信号。
func main() {
	flag.Parse()

	// Kratos logger 会自动把 trace.id/span.id 放进每条日志字段，
	// 后续 accesslog 和业务日志都复用同一个 logger，方便按 trace 关联排查。
	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
		"service.version", Version,
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)
	loggerHelper := log.NewHelper(logger)

	// 初始化 OpenTelemetry 导出器。
	// 未配置 TRACE_ENABLED 或 OTLP endpoint 时会返回 nil shutdown，不影响服务正常启动。
	shutdownTracer, err := observability.InitTracer(context.Background(), observability.TraceConfig{
		ServiceName:    Name,
		ServiceVersion: Version,
	})
	if err != nil {
		loggerHelper.Warnf("init OpenTelemetry tracer failed: %v", err)
	}
	if shutdownTracer != nil {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracer(ctx); err != nil {
				loggerHelper.Warnf("shutdown OpenTelemetry tracer failed: %v", err)
			}
		}()
	}

	// 加载配置文件。flagconf 可以是 configs 目录，也可以是具体 yaml 文件。
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	// 将配置反序列化到 protobuf 生成的 Bootstrap 结构，后续交给 Wire 注入各层构造函数。
	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}
	// wireApp 由 wire 生成，负责把 server/data/biz/service 层依赖全部串起来。
	app, cleanup, err := wireApp(bc.Server, bc.Registry, bc.Elasticsearch, bc.Data, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	// 启动 HTTP/gRPC Server 并阻塞等待退出信号。
	if err := app.Run(); err != nil {
		panic(err)
	}
}
