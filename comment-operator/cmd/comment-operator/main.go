package main

import (
	"flag"
	"os"

	"comment-operator/internal/conf"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/http"

	_ "go.uber.org/automaxprocs"
)

var (
	Name     = "comment-operator"
	Version  = "v1"
	flagconf string
	id, _    = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "./configs/config.yaml", "config path, eg: -conf ./configs/config.yaml")
}

func newApp(logger log.Logger, registrar registry.Registrar, hs *http.Server) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Logger(logger),
		kratos.Server(hs),
		kratos.Registrar(registrar),
	)
}

func main() {
	flag.Parse()

	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
		"service.version", Version,
		"trace.id", tracing.TraceID(),
		"span.id", tracing.SpanID(),
	)

	cfg, err := conf.Load(flagconf)
	if err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(cfg.Server.HTTP, cfg.Registry, cfg.Client.CommentService, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	if err := app.Run(); err != nil {
		panic(err)
	}
}
