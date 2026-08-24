package task

import (
	"comment-task/internal/conf"
	"comment-task/internal/data"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/segmentio/kafka-go"
)

const (
	// defaultPostLikeDirtyTopic 是点赞/取消点赞计数 dirty 通知的默认 topic。
	//
	// Canal 数据库变更继续使用 configs/config.yaml 中的 comment-service topic；
	// postlike 只承载“某个 post_id 需要刷 like_count”的轻量通知，避免污染 Canal 消息流。
	defaultPostLikeDirtyTopic = "postlike"
	// defaultPostLikeDirtyBroker 在本地未配置 Kafka broker 时兜底使用。
	defaultPostLikeDirtyBroker = "localhost:9092"
)

// CanalKafkaReader 包装 Canal 消息 reader，避免 Wire 在两个 *kafka.Reader provider 之间无法区分。
type CanalKafkaReader struct {
	*kafka.Reader
}

// PostLikeKafkaReader 包装点赞计数 dirty reader。
//
// 该 reader 只消费 postlike topic，点赞和取消点赞共用同一个 topic 与同一套 Redis delta 聚合。
type PostLikeKafkaReader struct {
	*kafka.Reader
}

// ProviderSet 声明后台任务模块的依赖注入入口。
//
// JobWorker 同时承载 Kafka -> ES 同步任务，以及 Redis -> MySQL 点赞计数刷库任务。
var ProviderSet = wire.NewSet(NewJobWorker, NewESClient, NewKafkaReader, NewPostLikeKafkaReader)

// NewJobWorker 创建后台任务 worker。
//
// data 参数提供 Redis/MySQL 资源，用于异步处理 service 写入的点赞/取消点赞计数 delta。
func NewJobWorker(canalKafka *CanalKafkaReader, postLikeKafka *PostLikeKafkaReader, es ESClient, data *data.Data, logger log.Logger) *JobWorker {
	return &JobWorker{
		kafkaReader:    canalKafka.Reader,
		postLikeReader: postLikeKafka.Reader,
		esClient:       es,
		data:           data,
		log:            log.NewHelper(logger),
	}
}

// NewKafkaReader 创建 Canal 消息消费者。
//
// 当前 Kafka 消息主要用于把 MySQL 变更同步到 Elasticsearch。
func NewKafkaReader(kafka2 *conf.Kafka) *CanalKafkaReader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafka2.Brokers,
		GroupID:  kafka2.GroupId, // 指定消费者组id
		Topic:    kafka2.Topic,
		MaxBytes: 10e6, // 10MB
	})
	return &CanalKafkaReader{Reader: r}
}

// NewPostLikeKafkaReader 创建点赞/取消点赞计数 dirty 消息消费者。
//
// 这个 reader 与 Canal reader 分离：Canal topic 只处理数据库变更，postlike topic 只处理计数刷库唤醒。
// topic/brokers 支持环境变量覆盖，方便本地和压测环境不改 proto 配置就能切换。
func NewPostLikeKafkaReader(kafka2 *conf.Kafka) *PostLikeKafkaReader {
	brokers := kafka2.Brokers
	if envBrokers := firstNonEmpty(os.Getenv("POST_LIKE_COUNT_KAFKA_BROKERS"), os.Getenv("KAFKA_BROKERS")); envBrokers != "" {
		brokers = splitCSV(envBrokers)
	}
	if len(brokers) == 0 {
		brokers = []string{defaultPostLikeDirtyBroker}
	}

	defaultGroupID := "comment-task-postlike"
	if kafka2.GroupId != "" {
		defaultGroupID = kafka2.GroupId + "-postlike"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  firstNonEmpty(os.Getenv("POST_LIKE_COUNT_KAFKA_GROUP_ID"), defaultGroupID),
		Topic:    firstNonEmpty(os.Getenv("POST_LIKE_COUNT_KAFKA_TOPIC"), defaultPostLikeDirtyTopic),
		MaxBytes: 10e6, // 10MB
	})
	return &PostLikeKafkaReader{Reader: r}
}

// NewESClient 创建 Elasticsearch typed client。
//
// 该 client 供 Kafka 消费逻辑写入 post/study_comment 索引。
func NewESClient(es *conf.Elasticsearch) ESClient {
	cfg := elasticsearch.Config{
		Addresses: es.Addresses,
	}

	// 创建客户端连接
	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return ESClient{}
	}
	return ESClient{
		esClient: client,
		Index:    es.Index,
	}
}

// firstNonEmpty 返回第一个非空字符串，用于环境变量默认值处理。
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// splitCSV 解析逗号分隔配置，并过滤空白项。
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}
