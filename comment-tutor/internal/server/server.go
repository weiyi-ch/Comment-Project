package server

import (
	stdhttp "net/http"

	"comment-tutor/internal/auth"
	"comment-tutor/internal/conf"
	"comment-tutor/internal/service"

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

func NewHTTPServer(cfg conf.HTTP, tutor *service.TutorCommentService, logger log.Logger) *http.Server {
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

	router := srv.Route("/api/tutor", tutorAuthFilter())
	router.GET("/posts", tutor.ListMyPosts)
	router.POST("/posts", tutor.CreatePost)
	router.GET("/posts/{post_id}", tutor.GetPostDetail)
	router.PUT("/posts/{post_id}", tutor.UpdatePost)
	router.DELETE("/posts/{post_id}", tutor.DeletePost)
	router.GET("/posts/{post_id}/comments", tutor.ListPostComments)
	router.GET("/comments/{comment_id}", tutor.GetCommentDetail)
	router.DELETE("/comments/{comment_id}", tutor.DeleteComment)
	router.POST("/comments/{comment_id}/reply", tutor.ReplyComment)
	router.DELETE("/replies/{reply_id}", tutor.DeleteReply)
	router.GET("/search/posts", tutor.SearchPosts)
	router.GET("/posts/{post_id}/comments/search", tutor.SearchComments)

	return srv
}

func tutorAuthFilter() http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			p, err := auth.ParseTutorPrincipal(r)
			if err != nil {
				stdhttp.Error(w, err.Error(), stdhttp.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
		})
	}
}
