package service

import (
	"context"

	pb "comment-service/api/comment/v1"
	"comment-service/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

type AuthService struct {
	pb.UnimplementedAuthServiceServer
	uc  *biz.AuthUsecase
	log *log.Helper
}

func NewAuthService(uc *biz.AuthUsecase, logger log.Logger) *AuthService {
	return &AuthService{
		uc:  uc,
		log: log.NewHelper(log.With(logger, "module", "service/auth")),
	}
}

func (s *AuthService) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.TokenReply, error) {
	reply, err := s.uc.Register(ctx, req.Username, req.Password, req.Role)
	return tokenReplyFromBiz(reply), err
}

func (s *AuthService) Login(ctx context.Context, req *pb.LoginRequest) (*pb.TokenReply, error) {
	reply, err := s.uc.Login(ctx, req.Username, req.Password, req.Role)
	return tokenReplyFromBiz(reply), err
}

func (s *AuthService) Refresh(ctx context.Context, req *pb.RefreshRequest) (*pb.TokenReply, error) {
	reply, err := s.uc.Refresh(ctx, req.RefreshToken, req.Role)
	return tokenReplyFromBiz(reply), err
}

func (s *AuthService) Logout(ctx context.Context, req *pb.LogoutRequest) (*pb.LogoutReply, error) {
	if err := s.uc.Logout(ctx, req.RefreshToken, req.Role); err != nil {
		return nil, err
	}
	return &pb.LogoutReply{}, nil
}

func tokenReplyFromBiz(reply *biz.AuthTokenReply) *pb.TokenReply {
	if reply == nil {
		return nil
	}
	return &pb.TokenReply{
		UserId:                reply.UserID,
		Role:                  reply.Role,
		AccessToken:           reply.AccessToken,
		AccessTokenExpiresAt:  reply.AccessTokenExpiresAt,
		RefreshToken:          reply.RefreshToken,
		RefreshTokenExpiresAt: reply.RefreshTokenExpiresAt,
	}
}
