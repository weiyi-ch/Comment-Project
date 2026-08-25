package data

import (
	"context"
	"fmt"

	"comment-service/dal/query"
	"comment-service/internal/conf"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// ProviderSet 声明 data 层可被 wire 注入的构造函数集合。
var ProviderSet = wire.NewSet(NewRedisClient, NewDB, NewES, NewData, NewAuthRepo, NewOperatorRepo, NewTutorRepo, NewStudentRepo, NewSearchRepo)

// Data 聚合 data 层需要访问的外部资源。
//
// q 是 gorm/gen 生成的查询入口，cache 是 Redis 客户端，es 是 Elasticsearch TypedClient。
type Data struct {
	db                  *gorm.DB
	q                   *query.Query
	cache               *redis.Client
	es                  *elasticsearch.TypedClient
	log                 *log.Helper
	postLikeDirtyWriter *postLikeDirtyWriter
}

// NewData 组装 data 层资源，并设置 gorm/gen 的默认数据库连接。
//
// 返回值：
// 1. *Data：业务仓储共用的 DB/Redis/ES 聚合对象；
// 2. cleanup：应用退出时由 Kratos 调用的资源清理函数；
// 3. error：当前构造过程没有额外错误，保留给 Wire 统一签名。
func NewData(db *gorm.DB, cache *redis.Client, es *elasticsearch.TypedClient, logger log.Logger) (*Data, func(), error) {
	helper := log.NewHelper(logger)
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	data := &Data{
		db:                  db,
		q:                   query.Q,
		cache:               cache,
		es:                  es,
		log:                 log.NewHelper(logger),
		postLikeDirtyWriter: newPostLikeDirtyWriterFromEnv(logger),
	}
	data.startCounterDirtyOutboxPublisher(outboxCtx)
	data.startBloomWarmup(outboxCtx)
	cleanup := func() {
		helper.Info("closing the data resources")
		cancelOutbox()
		if data.postLikeDirtyWriter != nil {
			if err := data.postLikeDirtyWriter.Close(); err != nil {
				helper.Warnf("close post like dirty kafka writer failed: %v", err)
			}
		}
	}
	// 为 gen 生成的 query 代码设置数据库对象，否则 query.Q 无法正常访问 MySQL。
	query.SetDefault(db)
	return data, cleanup, nil
}

// NewES 根据配置创建 Elasticsearch TypedClient。
//
// 返回 client 和 error；如果 ES 地址不可用，调用方会在启动时感知错误。
func NewES(cfg *conf.Elasticsearch) (*elasticsearch.TypedClient, error) {
	// GetAddresses 返回配置文件中的 ES 地址列表，TypedClient 负责后续搜索请求。
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{Addresses: cfg.GetAddresses()})
	if err != nil {
		fmt.Println("es链接失败")
	}
	return client, err
}

// NewDB 根据配置创建 GORM MySQL 连接。
//
// 返回 *gorm.DB 给 gorm/gen query 使用；错误通常代表 DSN 配置或 MySQL 连通性异常。
func NewDB(cfg *conf.Data) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(cfg.Database.GetSource()))
	return db, err
}

// NewRedisClient 根据配置创建 Redis 客户端。
//
// 返回值是惰性连接的 redis.Client，真正网络连接会在首次命令执行时建立。
func NewRedisClient(cfg *conf.Data) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Network:      cfg.Redis.Network,
		Password:     cfg.Redis.Pass,
		WriteTimeout: cfg.Redis.WriteTimeout.AsDuration(),
		ReadTimeout:  cfg.Redis.ReadTimeout.AsDuration(),
	})

}
