package server

import (
	stdhttp "net/http"

	"comment-operator/internal/auth"
	"comment-operator/internal/conf"
	"comment-operator/internal/service"

	consul "github.com/go-kratos/kratos/contrib/registry/consul/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

var ProviderSet = wire.NewSet(NewConsulRegistry, NewRegistrar, NewDiscovery, NewHTTPServer)

func NewConsulRegistry(cfg conf.Registry) *consul.Registry {
	c := api.DefaultConfig()
	c.Address = cfg.Consul.Address
	c.Scheme = cfg.Consul.Scheme
	client, err := api.NewClient(c)
	if err != nil {
		panic(err)
	}
	return consul.New(client)
}

func NewRegistrar(reg *consul.Registry) registry.Registrar {
	return reg
}

func NewDiscovery(reg *consul.Registry) registry.Discovery {
	return reg
}

func NewHTTPServer(cfg conf.HTTP, operator *service.OperatorCommentService, logger log.Logger) *http.Server {
	srv := http.NewServer(
		http.Address(cfg.Addr),
		http.Timeout(cfg.Timeout),
		http.Middleware(
			recovery.Recovery(),
			tracing.Server(),
		),
	)

	srv.HandleFunc("/health", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	router := srv.Route("/api/operator", operatorAuthFilter())
	router.GET("/posts/{post_id}", operator.GetPostDetail)
	router.GET("/comments", operator.ListComments)
	router.GET("/comments/{comment_id}", operator.GetCommentAuditDetail)
	router.POST("/comments/{comment_id}/audit", operator.AuditComment)
	router.GET("/search/comments", operator.SearchComments)

	return srv
}

func operatorAuthFilter() http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			p, err := auth.ParseOperatorPrincipal(r)
			if err != nil {
				stdhttp.Error(w, err.Error(), stdhttp.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
		})
	}
}
