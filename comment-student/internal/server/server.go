package server

import (
	stdhttp "net/http"

	"comment-student/internal/auth"
	"comment-student/internal/client"
	"comment-student/internal/conf"
	"comment-student/internal/ratelimit"
	"comment-student/internal/service"

	consul "github.com/go-kratos/kratos/contrib/registry/consul/v2"
	kerrors "github.com/go-kratos/kratos/v2/errors"
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

func NewHTTPServer(cfg conf.HTTP, authCfg conf.Auth, comment *client.CommentClient, student *service.StudentCommentService, limiter *ratelimit.Limiter, logger log.Logger) *http.Server {
	authSvc, err := auth.NewService(auth.Config{
		Role:          authCfg.Role,
		SigningSecret: authCfg.SigningSecret,
		AuthClient:    comment.Auth,
	})
	if err != nil {
		panic(err)
	}
	authHTTP := auth.NewHTTPService(authSvc)

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

	authRouter := srv.Route("/api/student/auth", limiter.AuthFilter())
	authRouter.POST("/register", authHTTP.Register)
	authRouter.POST("/login", authHTTP.Login)
	authRouter.POST("/refresh", authHTTP.Refresh)
	authRouter.POST("/logout", authHTTP.Logout)

	router := srv.Route("/api/student", studentAuthFilter(authSvc, limiter))
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

func studentAuthFilter(authSvc *auth.Service, limiter *ratelimit.Limiter) http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			p, err := authSvc.ParsePrincipal(r)
			if err != nil {
				stdhttp.Error(w, err.Error(), authErrorStatus(err))
				return
			}
			r = r.WithContext(auth.WithPrincipal(r.Context(), p))
			limiter.StudentFilter()(next).ServeHTTP(w, r)
		})
	}
}

func authErrorStatus(err error) int {
	if kerrors.IsForbidden(err) {
		return stdhttp.StatusForbidden
	}
	return stdhttp.StatusUnauthorized
}
