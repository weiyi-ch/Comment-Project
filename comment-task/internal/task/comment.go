package task

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"comment-task/internal/data"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
)

const (
	postIndex         = "post"
	studyCommentIndex = "study_comment"
)

type JobWorker struct {
	// kafkaReader 只消费 Canal 数据库变更 topic，用于同步 ES。
	kafkaReader *kafka.Reader
	// postLikeReader 只消费 postlike topic，用于唤醒点赞/取消点赞计数异步落库。
	postLikeReader *kafka.Reader
	esClient       ESClient
	// data 提供 Redis/MySQL 资源，供点赞计数异步落库任务使用。
	data *data.Data
	log  *log.Helper
}

type ESClient struct {
	esClient *elasticsearch.TypedClient
	Index    string
}

type Msg struct {
	Type            string                   `json:"type"`
	Database        string                   `json:"database"`
	Table           string                   `json:"table"`
	IsDdl           bool                     `json:"isDdl"`
	Data            []map[string]interface{} `json:"data"`
	BinlogFile      string                   `json:"binlog_file"`
	BinlogPos       int64                    `json:"binlog_pos"`
	BinlogFileCamel string                   `json:"binlogFile"`
	BinlogPosCamel  int64                    `json:"binlogPos"`
	Es              int64                    `json:"es"`
	Ts              int64                    `json:"ts"`
}

func (js *JobWorker) Start(ctx context.Context) error {
	js.log.Debug("JobWorker start...")
	// postlike topic 是帖子计数 dirty 通知通道；消息只负责唤醒调度，真正刷库由 Redis zset 扫描器推进。
	js.startPostLikeCountKafkaConsumer(ctx)
	js.startPostLikeCountFallbackFlusher(ctx)
	js.startPostCommentCountFallbackFlusher(ctx)
	js.startESSyncRetryWorker(ctx)

	for {
		select {
		case <-ctx.Done():
			js.log.Debug("JobWorker context canceled")
			return ctx.Err()
		default:
		}

		// 1. 从 Kafka 获取数据库变更。这里使用 FetchMessage + CommitMessages 手动提交 offset：
		// ES 写成功，或者失败事件已可靠写入 retry 表后才提交。
		m, err := js.kafkaReader.FetchMessage(ctx)
		if err != nil {
			js.log.Errorf("read from kafka error: %v", err)
			return err
		}

		if err := js.handleCanalKafkaMessage(ctx, m); err != nil {
			js.log.Errorf("handle canal kafka message failed: %v", err)
			return err
		}

		if err := js.kafkaReader.CommitMessages(ctx, m); err != nil {
			js.log.Errorf("commit canal kafka message failed: %v", err)
			return err
		}
	}
}

func (js *JobWorker) Stop(ctx context.Context) error {
	js.log.Debug("JobWorker stop...")
	// 进程退出前尽量再刷一批到期帖子计数，减少 Redis pending delta 的滞留时间。
	if flushed, err := js.flushPostLikeCountDeltas(ctx, postLikeCountFlushBatchSize); err != nil {
		js.log.Warnf("flush post like count deltas on stop failed: %v", err)
	} else if flushed > 0 {
		js.log.Infof("flush post like count deltas on stop success, count=%d", flushed)
	}
	if flushed, err := js.flushPostCommentCountDeltas(ctx, postLikeCountFlushBatchSize); err != nil {
		js.log.Warnf("flush post comment count deltas on stop failed: %v", err)
	} else if flushed > 0 {
		js.log.Infof("flush post comment count deltas on stop success, count=%d", flushed)
	}
	var firstErr error
	if js.postLikeReader != nil {
		firstErr = js.postLikeReader.Close()
	}
	if js.kafkaReader != nil {
		if err := js.kafkaReader.Close(); firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return firstErr
}

// getDocIDAndIndex 根据表名提取 ES 文档 ID 和目标索引。
func getDocIDAndIndex(table string, row map[string]interface{}) (string, string, bool) {
	switch table {
	case "post":
		postID := toInt64(row["post_id"])
		if postID <= 0 {
			return "", "", false
		}
		return strconv.FormatInt(postID, 10), postIndex, true

	case "study_comment":
		commentID := toInt64(row["comment_id"])
		if commentID <= 0 {
			return "", "", false
		}
		return strconv.FormatInt(commentID, 10), studyCommentIndex, true

	default:
		return "", "", false
	}
}

// indexDocument 索引文档。
func (js *JobWorker) indexDocument(ctx context.Context, docID string, data map[string]interface{}, targetIndex string) error {
	resp, err := js.esClient.esClient.Index(targetIndex).
		Id(docID).
		Document(data).
		Do(ctx)

	if err != nil {
		return err
	}

	js.log.Debugf("写入 ES 成功 [INSERT], index=%s, id=%s, result=%v", targetIndex, docID, resp.Result)
	return nil
}

// toInt64 把 Canal 里的字符串数字转为 int64。
// 例如："10004753614376960" -> int64(10004753614376960)
func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}

	switch t := v.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case int64:
		return t
	case uint:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		if t > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(t)
	case float64:
		// 如果 Canal 输出的是 JSON number，大雪花 ID 可能已经精度丢失。
		// 正常 Canal 输出一般是 string，所以这里只是兜底。
		return int64(t)
	case json.Number:
		n, err := t.Int64()
		if err != nil {
			return 0
		}
		return n
	case string:
		s := strings.TrimSpace(t)
		if s == "" || strings.EqualFold(s, "null") {
			return 0
		}

		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0
		}

		return n
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" || strings.EqualFold(s, "null") {
			return 0
		}

		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0
		}

		return n
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}
