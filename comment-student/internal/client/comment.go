package client

import (
	"context"
	"fmt"

	commentv1 "comment-student/api/comment/v1"
	"comment-student/internal/conf"
	"comment-student/internal/mtls"

	"github.com/go-kratos/kratos/v2/middleware/tracing"
	"github.com/go-kratos/kratos/v2/registry"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/google/wire"
	"google.golang.org/grpc"
)

var ProviderSet = wire.NewSet(NewCommentClient)

type CommentClient struct {
	conn    *grpc.ClientConn
	Student commentv1.StudentServiceClient
	Search  commentv1.SearchServiceClient
}

func NewCommentClient(cfg conf.CommentService, discovery registry.Discovery) (*CommentClient, func(), error) {
	endpoint := cfg.Endpoint
	opts := []kgrpc.ClientOption{
		kgrpc.WithTimeout(cfg.Timeout),
		kgrpc.WithMiddleware(tracing.Client()),
	}
	if endpoint == "" {
		endpoint = fmt.Sprintf("discovery:///%s", cfg.ServiceName)
		opts = append(opts, kgrpc.WithDiscovery(discovery))
	}
	opts = append([]kgrpc.ClientOption{kgrpc.WithEndpoint(endpoint)}, opts...)

	var (
		conn *grpc.ClientConn
		err  error
	)
	if cfg.TLS.Enabled {
		tlsConf, tlsErr := mtls.ClientConfig(cfg.TLS.CAFile, cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.ServerName)
		if tlsErr != nil {
			return nil, nil, tlsErr
		}
		opts = append(opts, kgrpc.WithTLSConfig(tlsConf))
		conn, err = kgrpc.Dial(context.Background(), opts...)
	} else {
		conn, err = kgrpc.DialInsecure(context.Background(), opts...)
	}
	if err != nil {
		return nil, nil, err
	}
	c := &CommentClient{
		conn:    conn,
		Student: commentv1.NewStudentServiceClient(conn),
		Search:  commentv1.NewSearchServiceClient(conn),
	}
	return c, func() {
		_ = c.Close()
	}, nil
}

func (c *CommentClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
