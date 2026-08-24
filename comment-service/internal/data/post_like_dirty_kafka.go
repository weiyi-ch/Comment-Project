package data

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
)

const (
	// postLikeDirtyEventType 是 Kafka 消息类型，task 通过它区分点赞计数脏通知和 Canal 消息。
	postLikeDirtyEventType    = "post_like_count_dirty"
	postCommentDirtyEventType = "post_comment_count_dirty"
	// defaultPostLikeDirtyTopic 是点赞/取消点赞计数 dirty 通知的独立 topic。
	//
	// Canal 数据库变更继续使用 comment-service topic，postlike 只承载点赞计数脏通知。
	defaultPostLikeDirtyTopic = "postlike"
	// defaultPostLikeDirtyBrokers 是本地压测 Kafka 容器的默认地址，可通过环境变量覆盖。
	defaultPostLikeDirtyBrokers = "localhost:9092"
	// postLikeDirtyNotifyTimeout 限制请求链路等待 Kafka 通知的时间。
	//
	// Kafka 只是唤醒 task 的通知通道；Redis dirty set 仍会兜底，所以不能让通知发送拖慢接口。
	postLikeDirtyNotifyTimeout = 100 * time.Millisecond
)

// postLikeDirtyMessage 是 service 发送给 task 的 Kafka 脏通知消息。
//
// post_id 用字符串保存，避免雪花 ID 在跨语言 JSON number 中出现精度风险。
type postLikeDirtyMessage struct {
	Type   string `json:"type"`
	PostID string `json:"post_id"`
}

// postLikeDirtyWriter 封装点赞计数脏通知的 Kafka writer。
//
// 它只发送“某个 post_id 已经从 clean 变 dirty”的通知，不承载真实 delta；真实 delta 在 Redis hash 中。
type postLikeDirtyWriter struct {
	writer *kafka.Writer
	log    *log.Helper
}

// newPostLikeDirtyWriterFromEnv 根据环境变量创建 Kafka writer。
//
// 支持的环境变量：
// - POST_LIKE_COUNT_KAFKA_DISABLED=true：关闭 Kafka 通知，保留 Redis dirty set 兜底。
// - POST_LIKE_COUNT_KAFKA_BROKERS 或 KAFKA_BROKERS：逗号分隔 broker 列表。
// - POST_LIKE_COUNT_KAFKA_TOPIC：通知消息 topic，默认 postlike。
func newPostLikeDirtyWriterFromEnv(logger log.Logger) *postLikeDirtyWriter {
	if strings.EqualFold(os.Getenv("POST_LIKE_COUNT_KAFKA_DISABLED"), "true") {
		return nil
	}

	brokers := splitCSV(firstNonEmpty(os.Getenv("POST_LIKE_COUNT_KAFKA_BROKERS"), os.Getenv("KAFKA_BROKERS"), defaultPostLikeDirtyBrokers))
	if len(brokers) == 0 {
		return nil
	}

	topic := firstNonEmpty(os.Getenv("POST_LIKE_COUNT_KAFKA_TOPIC"), defaultPostLikeDirtyTopic)
	helper := log.NewHelper(log.With(logger, "module", "data/post_like_dirty_kafka"))
	return &postLikeDirtyWriter{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireOne,
		},
		log: helper,
	}
}

// Notify 发送某个帖子需要刷点赞计数的 Kafka 通知。
//
// Kafka 消息 key 使用 post_id，便于同一帖子的通知尽量落到同一 partition。
func (w *postLikeDirtyWriter) Notify(ctx context.Context, postID int64) error {
	return w.notify(ctx, postLikeDirtyEventType, postID)
}

func (w *postLikeDirtyWriter) NotifyCommentCount(ctx context.Context, postID int64) error {
	return w.notify(ctx, postCommentDirtyEventType, postID)
}

func (w *postLikeDirtyWriter) notify(ctx context.Context, eventType string, postID int64) error {
	if w == nil || w.writer == nil {
		return nil
	}

	postIDText := strconv.FormatInt(postID, 10)
	msg := postLikeDirtyMessage{
		Type:   eventType,
		PostID: postIDText,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	notifyCtx, cancel := context.WithTimeout(ctx, postLikeDirtyNotifyTimeout)
	defer cancel()

	return w.writer.WriteMessages(notifyCtx, kafka.Message{
		Key:   []byte(postIDText),
		Value: payload,
	})
}

// Close 关闭 Kafka writer。
func (w *postLikeDirtyWriter) Close() error {
	if w == nil || w.writer == nil {
		return nil
	}
	return w.writer.Close()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
