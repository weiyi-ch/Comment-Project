package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	stdhttp "net/http"
	"strings"
	"time"

	commentv1 "comment-tutor/api/comment/v1"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/transport/http"
)

const tutorRole = "tutor"

type Config struct {
	Role          string
	SigningSecret string
	AuthClient    commentv1.AuthServiceClient
}

type contextKey struct{}

type Principal struct {
	UserID  int64
	Role    string
	TokenID string
}

type Service struct {
	cfg Config
}

type HTTPService struct {
	auth *Service
}

type registerRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type logoutRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type claims struct {
	TokenID   string `json:"token_id"`
	UserID    int64  `json:"user_id"`
	Role      string `json:"role"`
	ExpiresAt int64  `json:"expires_at"`
	IssuedAt  int64  `json:"issued_at"`
}

func NewService(cfg Config) (*Service, error) {
	if cfg.Role == "" {
		cfg.Role = tutorRole
	}
	if cfg.SigningSecret == "" {
		cfg.SigningSecret = "comment-tutor-dev-secret"
	}
	if cfg.AuthClient == nil {
		return nil, kerrors.InternalServer("AUTH_CLIENT_MISSING", "auth service client is required")
	}
	return &Service{cfg: cfg}, nil
}

func NewHTTPService(auth *Service) *HTTPService {
	return &HTTPService{auth: auth}
}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, p)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(contextKey{}).(Principal)
	return p, ok
}

func RequireTutor(ctx context.Context) (Principal, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok || p.UserID <= 0 {
		return Principal{}, kerrors.Unauthorized("UNAUTHORIZED", "missing tutor identity")
	}
	if p.Role != tutorRole {
		return Principal{}, kerrors.Forbidden("FORBIDDEN", "tutor role required")
	}
	return p, nil
}

func (s *Service) ParsePrincipal(r *stdhttp.Request) (Principal, error) {
	token, err := bearerToken(r)
	if err != nil {
		return Principal{}, err
	}
	c, err := s.verifyAccessToken(token)
	if err != nil {
		return Principal{}, err
	}
	if c.Role != s.cfg.Role {
		return Principal{}, kerrors.Forbidden("FORBIDDEN", s.cfg.Role+" role required")
	}
	return Principal{UserID: c.UserID, Role: c.Role, TokenID: c.TokenID}, nil
}

func (h *HTTPService) Register(ctx http.Context) error {
	var req registerRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	reply, err := h.auth.cfg.AuthClient.Register(ctx, &commentv1.RegisterRequest{
		Username: req.Username,
		Password: req.Password,
		Role:     h.auth.cfg.Role,
	})
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Login(ctx http.Context) error {
	var req loginRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	reply, err := h.auth.cfg.AuthClient.Login(ctx, &commentv1.LoginRequest{
		Username: req.Username,
		Password: req.Password,
		Role:     h.auth.cfg.Role,
	})
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Refresh(ctx http.Context) error {
	var req refreshRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	reply, err := h.auth.cfg.AuthClient.Refresh(ctx, &commentv1.RefreshRequest{
		RefreshToken: req.RefreshToken,
		Role:         h.auth.cfg.Role,
	})
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Logout(ctx http.Context) error {
	var req logoutRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if _, err := h.auth.cfg.AuthClient.Logout(ctx, &commentv1.LogoutRequest{
		RefreshToken: req.RefreshToken,
		Role:         h.auth.cfg.Role,
	}); err != nil {
		return err
	}
	return ctx.Result(204, nil)
}

func (s *Service) verifyAccessToken(token string) (claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return claims{}, kerrors.Unauthorized("UNAUTHORIZED", "invalid access token")
	}
	input := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(s.cfg.SigningSecret))
	_, _ = mac.Write([]byte(input))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return claims{}, kerrors.Unauthorized("UNAUTHORIZED", "invalid access token signature")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims{}, kerrors.Unauthorized("UNAUTHORIZED", "invalid access token payload")
	}
	var c claims
	if err := json.Unmarshal(body, &c); err != nil {
		return claims{}, kerrors.Unauthorized("UNAUTHORIZED", "invalid access token claims")
	}
	if c.UserID <= 0 || c.Role == "" || c.TokenID == "" || time.Now().Unix() >= c.ExpiresAt {
		return claims{}, kerrors.Unauthorized("UNAUTHORIZED", "access token expired or incomplete")
	}
	return c, nil
}

func bearerToken(r *stdhttp.Request) (string, error) {
	authHeader := strings.TrimSpace(r.Header.Get("authorization"))
	if authHeader == "" {
		return "", kerrors.Unauthorized("UNAUTHORIZED", "missing authorization header")
	}
	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	token = strings.TrimSpace(strings.TrimPrefix(token, "bearer "))
	if token == "" || token == authHeader {
		return "", kerrors.Unauthorized("UNAUTHORIZED", "missing bearer token")
	}
	return token, nil
}
