package server

import (
	stdhttp "net/http"

	"comment-student/internal/auth"
	"comment-student/internal/conf"
	"comment-student/internal/service"

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

func NewHTTPServer(cfg conf.HTTP, student *service.StudentCommentService, logger log.Logger) *http.Server {
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

	router := srv.Route("/api/student", studentAuthFilter())
	router.GET("/posts/{post_id}", student.GetPostDetail)
	router.GET("/posts/{post_id}/comments", student.ListPostComments)
	router.POST("/posts/{post_id}/comments", student.CreateComment)
	router.GET("/comments", student.ListMyComments)
	router.GET("/comments/{comment_id}", student.GetCommentDetail)
	router.DELETE("/comments/{comment_id}", student.DeleteMyComment)
	router.POST("/posts/{post_id}/like", student.LikePost)
	router.DELETE("/posts/{post_id}/like", student.UnlikePost)
	router.GET("/search/posts", student.SearchPosts)
	router.GET("/search/comments", student.SearchComments)

	return srv
}

func studentAuthFilter() http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			p, err := auth.ParseStudentPrincipal(r)
			if err != nil {
				stdhttp.Error(w, err.Error(), stdhttp.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
		})
	}
}
