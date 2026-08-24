package data

import (
	"database/sql"
	"time"

	"comment-task/internal/conf"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// ProviderSet 声明 task 层可注入的数据资源。
//
// comment-task 当前既消费 Kafka 同步 ES，也需要 Redis/MySQL 来异步刷点赞计数。
var ProviderSet = wire.NewSet(NewDB, NewRedisClient, NewData, NewGreeterRepo)

// Data 聚合后台任务依赖的外部数据资源。
//
// db 用于把 Redis 中聚合后的计数 delta 落到 post_counter，cache 用于读取 service 写入的队列。
type Data struct {
	db    *gorm.DB
	cache *redis.Client
	log   *log.Helper
}

// NewData 创建后台任务共享的数据资源对象，并返回 Kratos 退出时的清理函数。
func NewData(db *gorm.DB, cache *redis.Client, logger log.Logger) (*Data, func(), error) {
	helper := log.NewHelper(logger)
	data := &Data{db: db, cache: cache, log: helper}
	cleanup := func() {
		helper.Info("closing the data resources")
		// Redis 和 MySQL 都由 task 进程持有连接，进程退出时主动关闭，避免连接泄漏。
		if cache != nil {
			if err := cache.Close(); err != nil {
				helper.Warnf("close redis failed: %v", err)
			}
		}
		closeDB(db, helper)
	}
	return data, cleanup, nil
}

// NewDB 根据配置创建 GORM MySQL 连接。
//
// 点赞/评论计数后台任务通过该连接批量更新 post_counter，避免更新 post 主表。
func NewDB(cfg *conf.Data) (*gorm.DB, error) {
	return gorm.Open(mysql.Open(cfg.GetDatabase().GetSource()))
}

// NewRedisClient 根据配置创建 Redis 客户端。
//
// Redis 中保存 service 写入的 post_like delta 和 dirty 队列。
func NewRedisClient(cfg *conf.Data) *redis.Client {
	redisCfg := cfg.GetRedis()
	var readTimeout, writeTimeout time.Duration
	// 配置文件中的 duration 字段可能为空，手动判断后再转成 time.Duration。
	if timeout := redisCfg.GetReadTimeout(); timeout != nil {
		readTimeout = timeout.AsDuration()
	}
	if timeout := redisCfg.GetWriteTimeout(); timeout != nil {
		writeTimeout = timeout.AsDuration()
	}

	return redis.NewClient(&redis.Options{
		Addr:         redisCfg.GetAddr(),
		Network:      redisCfg.GetNetwork(),
		Password:     redisCfg.GetPass(),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	})
}

// DB 返回后台任务使用的 MySQL 连接。
func (d *Data) DB() *gorm.DB {
	if d == nil {
		return nil
	}
	return d.db
}

// Redis 返回后台任务使用的 Redis 客户端。
func (d *Data) Redis() *redis.Client {
	if d == nil {
		return nil
	}
	return d.cache
}

// closeDB 关闭 GORM 底层的 database/sql 连接池。
func closeDB(db *gorm.DB, logger *log.Helper) {
	if db == nil {
		return
	}

	sqlDB, err := db.DB()
	if err != nil {
		logger.Warnf("get sql db failed: %v", err)
		return
	}
	if err := sqlDB.Close(); err != nil && err != sql.ErrConnDone {
		logger.Warnf("close db failed: %v", err)
	}
}
