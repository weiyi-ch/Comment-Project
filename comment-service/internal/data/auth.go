package data

import (
	"context"
	"errors"
	"time"

	"comment-service/internal/biz"
	"comment-service/pkg/snowflake"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type userAccount struct {
	ID           int64     `gorm:"column:id;primaryKey"`
	UserID       int64     `gorm:"column:user_id"`
	Username     string    `gorm:"column:username"`
	Role         string    `gorm:"column:role"`
	PasswordHash string    `gorm:"column:password_hash"`
	Status       int32     `gorm:"column:status"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (userAccount) TableName() string {
	return "user_account"
}

type refreshTokenSession struct {
	ID         int64     `gorm:"column:id;primaryKey"`
	TokenID    string    `gorm:"column:token_id"`
	UserID     int64     `gorm:"column:user_id"`
	Role       string    `gorm:"column:role"`
	TokenHash  string    `gorm:"column:token_hash"`
	ExpiresAt  time.Time `gorm:"column:expires_at"`
	Revoked    int32     `gorm:"column:revoked"`
	ReplacedBy string    `gorm:"column:replaced_by"`
	CreatedAt  time.Time `gorm:"column:created_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

func (refreshTokenSession) TableName() string {
	return "refresh_token_session"
}

type authRepo struct {
	data *Data
	log  *log.Helper
}

func NewAuthRepo(data *Data, logger log.Logger) biz.AuthRepo {
	return &authRepo{
		data: data,
		log:  log.NewHelper(log.With(logger, "module", "data/auth")),
	}
}

func (r *authRepo) CreateUser(ctx context.Context, username, role, passwordHash string) (*biz.AuthUser, error) {
	var created userAccount
	err := r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		created = userAccount{
			UserID:       snowflake.GenID(),
			Username:     username,
			Role:         role,
			PasswordHash: passwordHash,
			Status:       1,
		}
		return tx.Create(&created).Error
	})
	if err != nil {
		return nil, err
	}
	return authUserFromData(created), nil
}

func (r *authRepo) GetUserByUsernameRole(ctx context.Context, username, role string) (*biz.AuthUser, error) {
	var user userAccount
	err := r.data.db.WithContext(ctx).
		Where("username = ? AND role = ?", username, role).
		First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return authUserFromData(user), nil
}

func (r *authRepo) CreateRefreshToken(ctx context.Context, token biz.RefreshTokenRecord) error {
	row := refreshTokenSession{
		TokenID:   token.TokenID,
		UserID:    token.UserID,
		Role:      token.Role,
		TokenHash: token.TokenHash,
		ExpiresAt: token.ExpiresAt,
		Revoked:   boolToInt32(token.Revoked),
	}
	return r.data.db.WithContext(ctx).Create(&row).Error
}

func (r *authRepo) RotateRefreshToken(ctx context.Context, oldTokenID, oldSecretHash string, next biz.RefreshTokenRecord) error {
	return r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var old refreshTokenSession
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("token_id = ?", oldTokenID).
			First(&old).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return biz.ErrInvalidRefreshToken
		}
		if err != nil {
			return err
		}
		if old.Revoked != 0 || old.TokenHash != oldSecretHash || time.Now().After(old.ExpiresAt) {
			return biz.ErrInvalidRefreshToken
		}

		nextRow := refreshTokenSession{
			TokenID:   next.TokenID,
			UserID:    next.UserID,
			Role:      next.Role,
			TokenHash: next.TokenHash,
			ExpiresAt: next.ExpiresAt,
			Revoked:   boolToInt32(next.Revoked),
		}
		if err := tx.Create(&nextRow).Error; err != nil {
			return err
		}
		return tx.Model(&old).
			Updates(map[string]any{
				"revoked":     1,
				"replaced_by": next.TokenID,
			}).Error
	})
}

func (r *authRepo) RevokeRefreshToken(ctx context.Context, tokenID, secretHash, role string) error {
	info := r.data.db.WithContext(ctx).Model(&refreshTokenSession{}).
		Where("token_id = ? AND token_hash = ? AND role = ?", tokenID, secretHash, role).
		Update("revoked", 1)
	if info.Error != nil {
		return info.Error
	}
	if info.RowsAffected == 0 {
		return biz.ErrInvalidRefreshToken
	}
	return nil
}

func (r *authRepo) GetRefreshToken(ctx context.Context, tokenID string) (*biz.RefreshTokenRecord, error) {
	var row refreshTokenSession
	err := r.data.db.WithContext(ctx).Where("token_id = ?", tokenID).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &biz.RefreshTokenRecord{
		TokenID:    row.TokenID,
		UserID:     row.UserID,
		Role:       row.Role,
		TokenHash:  row.TokenHash,
		ExpiresAt:  row.ExpiresAt,
		Revoked:    row.Revoked != 0,
		ReplacedBy: row.ReplacedBy,
		CreatedAt:  row.CreatedAt,
		UpdatedAt:  row.UpdatedAt,
	}, nil
}

func authUserFromData(user userAccount) *biz.AuthUser {
	return &biz.AuthUser{
		UserID:       user.UserID,
		Username:     user.Username,
		Role:         user.Role,
		PasswordHash: user.PasswordHash,
		Status:       user.Status,
		CreatedAt:    user.CreatedAt,
		UpdatedAt:    user.UpdatedAt,
	}
}

func boolToInt32(v bool) int32 {
	if v {
		return 1
	}
	return 0
}
