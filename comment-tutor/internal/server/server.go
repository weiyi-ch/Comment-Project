package server

import (
	stdhttp "net/http"

	"comment-tutor/internal/auth"
	"comment-tutor/internal/client"
	"comment-tutor/internal/conf"
	"comment-tutor/internal/service"

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

func NewHTTPServer(cfg conf.HTTP, authCfg conf.Auth, comment *client.CommentClient, tutor *service.TutorCommentService, logger log.Logger) *http.Server {
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

	authRouter := srv.Route("/api/tutor/auth")
	authRouter.POST("/register", authHTTP.Register)
	authRouter.POST("/login", authHTTP.Login)
	authRouter.POST("/refresh", authHTTP.Refresh)
	authRouter.POST("/logout", authHTTP.Logout)

	router := srv.Route("/api/tutor", tutorAuthFilter(authSvc))
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

func tutorAuthFilter(authSvc *auth.Service) http.FilterFunc {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			p, err := authSvc.ParsePrincipal(r)
			if err != nil {
				stdhttp.Error(w, err.Error(), authErrorStatus(err))
				return
			}
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
		})
	}
}

func authErrorStatus(err error) int {
	if kerrors.IsForbidden(err) {
		return stdhttp.StatusForbidden
	}
	return stdhttp.StatusUnauthorized
}
