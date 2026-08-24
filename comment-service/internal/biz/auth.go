package biz

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"golang.org/x/crypto/bcrypt"
)

const (
	RoleStudent  = "student"
	RoleTutor    = "tutor"
	RoleOperator = "operator"

	userStatusActive = 1
)

var ErrInvalidRefreshToken = errors.New("invalid refresh token")

type AuthUser struct {
	UserID       int64
	Username     string
	Role         string
	PasswordHash string
	Status       int32
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type RefreshTokenRecord struct {
	TokenID    string
	UserID     int64
	Role       string
	TokenHash  string
	ExpiresAt  time.Time
	Revoked    bool
	ReplacedBy string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type AuthTokenReply struct {
	UserID                int64
	Role                  string
	AccessToken           string
	AccessTokenExpiresAt  int64
	RefreshToken          string
	RefreshTokenExpiresAt int64
}

type AuthRepo interface {
	CreateUser(ctx context.Context, username, role, passwordHash string) (*AuthUser, error)
	GetUserByUsernameRole(ctx context.Context, username, role string) (*AuthUser, error)
	CreateRefreshToken(ctx context.Context, token RefreshTokenRecord) error
	GetRefreshToken(ctx context.Context, tokenID string) (*RefreshTokenRecord, error)
	RotateRefreshToken(ctx context.Context, oldTokenID, oldSecretHash string, next RefreshTokenRecord) error
	RevokeRefreshToken(ctx context.Context, tokenID, secretHash, role string) error
}

type AuthUsecase struct {
	repo            AuthRepo
	log             *log.Helper
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
	signingSecrets  map[string]string
}

type authClaims struct {
	TokenID   string `json:"token_id"`
	UserID    int64  `json:"user_id"`
	Role      string `json:"role"`
	ExpiresAt int64  `json:"expires_at"`
	IssuedAt  int64  `json:"issued_at"`
}

func NewAuthUsecase(repo AuthRepo, logger log.Logger) *AuthUsecase {
	return &AuthUsecase{
		repo:            repo,
		log:             log.NewHelper(log.With(logger, "module", "usecase/auth")),
		accessTokenTTL:  15 * time.Minute,
		refreshTokenTTL: 7 * 24 * time.Hour,
		signingSecrets: map[string]string{
			RoleStudent:  "comment-student-dev-secret",
			RoleTutor:    "comment-tutor-dev-secret",
			RoleOperator: "comment-operator-dev-secret",
		},
	}
}

func (uc *AuthUsecase) Register(ctx context.Context, username, password, role string) (*AuthTokenReply, error) {
	username, role = normalizeAuthInput(username, role)
	if err := validateUserPasswordRole(username, password, role); err != nil {
		return nil, err
	}
	existing, err := uc.repo.GetUserByUsernameRole(ctx, username, role)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, kerrors.Conflict("USER_EXISTS", "username already exists")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	user, err := uc.repo.CreateUser(ctx, username, role, string(hash))
	if err != nil {
		return nil, err
	}
	return uc.issueAndStoreTokens(ctx, user)
}

func (uc *AuthUsecase) Login(ctx context.Context, username, password, role string) (*AuthTokenReply, error) {
	username, role = normalizeAuthInput(username, role)
	if err := validateRole(role); err != nil {
		return nil, err
	}
	user, err := uc.repo.GetUserByUsernameRole(ctx, username, role)
	if err != nil {
		return nil, err
	}
	if user == nil || user.Status != userStatusActive || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid username or password")
	}
	return uc.issueAndStoreTokens(ctx, user)
}

func (uc *AuthUsecase) Refresh(ctx context.Context, rawRefreshToken, role string) (*AuthTokenReply, error) {
	role = strings.TrimSpace(role)
	if err := validateRole(role); err != nil {
		return nil, err
	}
	tokenID, secret, err := splitRefreshToken(rawRefreshToken)
	if err != nil {
		return nil, err
	}
	rt, err := uc.repo.GetRefreshToken(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	if rt == nil || rt.Role != role || rt.Revoked || time.Now().After(rt.ExpiresAt) || rt.TokenHash != hashToken(secret) {
		return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
	}
	user := &AuthUser{UserID: rt.UserID, Role: rt.Role, Status: userStatusActive}
	reply, next, err := uc.issueTokens(user)
	if err != nil {
		return nil, err
	}
	if err := uc.repo.RotateRefreshToken(ctx, tokenID, hashToken(secret), next); err != nil {
		if errors.Is(err, ErrInvalidRefreshToken) {
			return nil, kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
		}
		return nil, err
	}
	return reply, nil
}

func (uc *AuthUsecase) Logout(ctx context.Context, rawRefreshToken, role string) error {
	role = strings.TrimSpace(role)
	if err := validateRole(role); err != nil {
		return err
	}
	tokenID, secret, err := splitRefreshToken(rawRefreshToken)
	if err != nil {
		return err
	}
	if err := uc.repo.RevokeRefreshToken(ctx, tokenID, hashToken(secret), role); err != nil {
		if errors.Is(err, ErrInvalidRefreshToken) {
			return kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
		}
		return err
	}
	return nil
}

func (uc *AuthUsecase) issueAndStoreTokens(ctx context.Context, user *AuthUser) (*AuthTokenReply, error) {
	reply, refreshRecord, err := uc.issueTokens(user)
	if err != nil {
		return nil, err
	}
	if err := uc.repo.CreateRefreshToken(ctx, refreshRecord); err != nil {
		return nil, err
	}
	return reply, nil
}

func (uc *AuthUsecase) issueTokens(user *AuthUser) (*AuthTokenReply, RefreshTokenRecord, error) {
	now := time.Now()
	accessID := randomHex(16)
	accessExpiresAt := now.Add(uc.accessTokenTTL)
	access, err := uc.signAccessToken(user.Role, authClaims{
		TokenID:   accessID,
		UserID:    user.UserID,
		Role:      user.Role,
		IssuedAt:  now.Unix(),
		ExpiresAt: accessExpiresAt.Unix(),
	})
	if err != nil {
		return nil, RefreshTokenRecord{}, err
	}
	refreshID := randomHex(16)
	refreshSecret := randomHex(32)
	refreshExpiresAt := now.Add(uc.refreshTokenTTL)
	record := RefreshTokenRecord{
		TokenID:   refreshID,
		UserID:    user.UserID,
		Role:      user.Role,
		TokenHash: hashToken(refreshSecret),
		ExpiresAt: refreshExpiresAt,
	}
	return &AuthTokenReply{
		UserID:                user.UserID,
		Role:                  user.Role,
		AccessToken:           access,
		AccessTokenExpiresAt:  accessExpiresAt.Unix(),
		RefreshToken:          refreshID + "." + refreshSecret,
		RefreshTokenExpiresAt: refreshExpiresAt.Unix(),
	}, record, nil
}

func (uc *AuthUsecase) signAccessToken(role string, claims authClaims) (string, error) {
	secret := uc.signingSecrets[role]
	if secret == "" {
		return "", kerrors.BadRequest("BAD_REQUEST", "unsupported role")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	bodyData, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(bodyData)
	input := header + "." + body
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func validateUserPasswordRole(username, password, role string) error {
	if username == "" || len(username) > 64 || len(password) < 8 || len(password) > 128 {
		return kerrors.BadRequest("BAD_REQUEST", "username is required and password must be 8-128 characters")
	}
	return validateRole(role)
}

func validateRole(role string) error {
	switch role {
	case RoleStudent, RoleTutor, RoleOperator:
		return nil
	default:
		return kerrors.BadRequest("BAD_REQUEST", "unsupported role")
	}
}

func normalizeAuthInput(username, role string) (string, string) {
	return strings.TrimSpace(username), strings.TrimSpace(role)
}

func splitRefreshToken(token string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", kerrors.Unauthorized("UNAUTHORIZED", "invalid refresh token")
	}
	return parts[0], parts[1], nil
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
