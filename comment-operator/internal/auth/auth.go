package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/transport/http"
	"golang.org/x/crypto/bcrypt"
)

const operatorRole = "operator"

type Config struct {
	Role            string
	SigningSecret   string
	StoreFile       string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

type contextKey struct{}

type Principal struct {
	UserID  int64
	Role    string
	TokenID string
}

type User struct {
	UserID       int64     `json:"user_id"`
	Username     string    `json:"username"`
	Role         string    `json:"role"`
	PasswordHash string    `json:"password_hash"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type RefreshToken struct {
	TokenID    string    `json:"token_id"`
	UserID     int64     `json:"user_id"`
	Role       string    `json:"role"`
	TokenHash  string    `json:"token_hash"`
	ExpiresAt  time.Time `json:"expires_at"`
	Revoked    bool      `json:"revoked"`
	ReplacedBy string    `json:"replaced_by,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type storeData struct {
	NextUserID    int64                    `json:"next_user_id"`
	Users         map[int64]*User          `json:"users"`
	UsernameIndex map[string]int64         `json:"username_index"`
	RefreshTokens map[string]*RefreshToken `json:"refresh_tokens"`
}

type Service struct {
	cfg   Config
	mu    sync.Mutex
	store storeData
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

type tokenReply struct {
	UserID                int64  `json:"user_id"`
	Role                  string `json:"role"`
	AccessToken           string `json:"access_token"`
	AccessTokenExpiresAt  int64  `json:"access_token_expires_at"`
	RefreshToken          string `json:"refresh_token"`
	RefreshTokenExpiresAt int64  `json:"refresh_token_expires_at"`
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
		cfg.Role = operatorRole
	}
	if cfg.SigningSecret == "" {
		cfg.SigningSecret = "dev-only-change-me"
	}
	if cfg.StoreFile == "" {
		cfg.StoreFile = "data/comment-operator-auth.json"
	}
	if cfg.AccessTokenTTL <= 0 {
		cfg.AccessTokenTTL = 15 * time.Minute
	}
	if cfg.RefreshTokenTTL <= 0 {
		cfg.RefreshTokenTTL = 7 * 24 * time.Hour
	}
	s := &Service{cfg: cfg}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
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

func RequireOperator(ctx context.Context) (Principal, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok || p.UserID <= 0 {
		return Principal{}, kerrors.Unauthorized("UNAUTHORIZED", "missing operator identity")
	}
	if p.Role != operatorRole {
		return Principal{}, kerrors.Forbidden("FORBIDDEN", "operator role required")
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
	reply, err := h.auth.Register(req.Username, req.Password)
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Login(ctx http.Context) error {
	var req loginRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	reply, err := h.auth.Login(req.Username, req.Password)
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Refresh(ctx http.Context) error {
	var req refreshRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	reply, err := h.auth.Refresh(req.RefreshToken)
	return ctx.Returns(reply, err)
}

func (h *HTTPService) Logout(ctx http.Context) error {
	var req logoutRequest
	if err := ctx.Bind(&req); err != nil {
		return kerrors.BadRequest("BAD_REQUEST", err.Error())
	}
	if err := h.auth.Logout(req.RefreshToken); err != nil {
		return err
	}
	return ctx.Result(204, nil)
}

func (s *Service) Register(username, password string) (*tokenReply, error) {
	username = strings.TrimSpace(username)
	if username == "" || len(password) < 8 {
		return nil, kerrors.BadRequest("BAD_REQUEST", "username is required and password must be at least 8 characters")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.store.UsernameIndex[usernameKey(s.cfg.Role, username)]; exists {
		return nil, kerrors.Conflict("USER_EXISTS", "username already exists")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	s.store.NextUserID++
	user := &User{
		UserID:       s.store.NextUserID,
		Username:     username,
		Role:         s.cfg.Role,
		PasswordHash: string(hash),
		Status:       "active",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.store.Users[user.UserID] = user
	s.store.UsernameIndex[usernameKey(user.Role, user.Username)] = user.UserID
	reply, err := s.issueTokensLocked(user)
	if err != nil {
		return nil, err
	}
	return reply, s.saveLocked()
}

func (s *Service) Login(username, password string) (*tokenReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	user, err := s.userByUsernameLocked(username)
	if err != nil {
		return nil, err
	}
	if user.Status != "active" || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid username or password")
	}
	reply, err := s.issueTokensLocked(user)
	if err != nil {
		return nil, err
	}
	return reply, s.saveLocked()
}

func (s *Service) Refresh(rawRefreshToken string) (*tokenReply, error) {
	tokenID, secret, err := splitRefreshToken(rawRefreshToken)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.store.RefreshTokens[tokenID]
	if !ok || rt.Revoked || time.Now().After(rt.ExpiresAt) || rt.TokenHash != hashToken(secret) {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
	}
	user := s.store.Users[rt.UserID]
	if user == nil || user.Role != s.cfg.Role || user.Status != "active" {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token owner")
	}
	rt.Revoked = true
	rt.UpdatedAt = time.Now()
	reply, err := s.issueTokensLocked(user)
	if err != nil {
		return nil, err
	}
	rt.ReplacedBy = latestRefreshTokenID(reply.RefreshToken)
	return reply, s.saveLocked()
}

func (s *Service) Logout(rawRefreshToken string) error {
	tokenID, secret, err := splitRefreshToken(rawRefreshToken)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.store.RefreshTokens[tokenID]
	if !ok || rt.TokenHash != hashToken(secret) {
		return kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
	}
	rt.Revoked = true
	rt.UpdatedAt = time.Now()
	return s.saveLocked()
}

func (s *Service) issueTokensLocked(user *User) (*tokenReply, error) {
	now := time.Now()
	accessID := randomHex(16)
	accessExpiresAt := now.Add(s.cfg.AccessTokenTTL)
	access, err := s.signAccessToken(claims{
		TokenID:   accessID,
		UserID:    user.UserID,
		Role:      user.Role,
		IssuedAt:  now.Unix(),
		ExpiresAt: accessExpiresAt.Unix(),
	})
	if err != nil {
		return nil, err
	}
	refreshID := randomHex(16)
	refreshSecret := randomHex(32)
	refreshExpiresAt := now.Add(s.cfg.RefreshTokenTTL)
	s.store.RefreshTokens[refreshID] = &RefreshToken{
		TokenID:   refreshID,
		UserID:    user.UserID,
		Role:      user.Role,
		TokenHash: hashToken(refreshSecret),
		ExpiresAt: refreshExpiresAt,
		CreatedAt: now,
		UpdatedAt: now,
	}
	return &tokenReply{
		UserID:                user.UserID,
		Role:                  user.Role,
		AccessToken:           access,
		AccessTokenExpiresAt:  accessExpiresAt.Unix(),
		RefreshToken:          refreshID + "." + refreshSecret,
		RefreshTokenExpiresAt: refreshExpiresAt.Unix(),
	}, nil
}

func (s *Service) signAccessToken(c claims) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyData, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(bodyData)
	input := header + "." + body
	mac := hmac.New(sha256.New, []byte(s.cfg.SigningSecret))
	_, _ = mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
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

func (s *Service) userByUsernameLocked(username string) (*User, error) {
	id, ok := s.store.UsernameIndex[usernameKey(s.cfg.Role, strings.TrimSpace(username))]
	if !ok {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid username or password")
	}
	user := s.store.Users[id]
	if user == nil {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid username or password")
	}
	return user, nil
}

func (s *Service) load() error {
	s.store = storeData{
		Users:         map[int64]*User{},
		UsernameIndex: map[string]int64{},
		RefreshTokens: map[string]*RefreshToken{},
	}
	data, err := os.ReadFile(s.cfg.StoreFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.store)
}

func (s *Service) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.cfg.StoreFile), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.cfg.StoreFile, data, 0600)
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

func splitRefreshToken(token string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
	}
	return parts[0], parts[1], nil
}

func latestRefreshTokenID(token string) string {
	id, _, _ := splitRefreshToken(token)
	return id
}

func usernameKey(role, username string) string {
	return role + ":" + strings.ToLower(strings.TrimSpace(username))
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf)
}
