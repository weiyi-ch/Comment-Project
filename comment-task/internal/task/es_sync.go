package task

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

const (
	esRetryMaxAttempts   = 3
	esRetryBatchSize     = 50
	esRetryInterval      = 5 * time.Second
	esRetryBackoffBase   = 5 * time.Second
	esSyncRetryTableDDL  = "CREATE TABLE IF NOT EXISTS es_sync_retry (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, event_type VARCHAR(16) NOT NULL, doc_id VARCHAR(64) NOT NULL, target_index VARCHAR(64) NOT NULL, binlog_file VARCHAR(128) NOT NULL, binlog_pos BIGINT NOT NULL, payload JSON NOT NULL, retry_count INT NOT NULL DEFAULT 0, next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_es_sync_retry_due (next_retry_at), KEY idx_es_sync_retry_doc (target_index, doc_id))"
	esSyncDLQTableDDL    = "CREATE TABLE IF NOT EXISTS es_sync_dlq (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, event_type VARCHAR(16) NOT NULL, doc_id VARCHAR(64) NOT NULL, target_index VARCHAR(64) NOT NULL, binlog_file VARCHAR(128) NOT NULL, binlog_pos BIGINT NOT NULL, payload JSON NOT NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
	esSyncRetryInsertSQL = "INSERT INTO es_sync_retry(table_name, event_type, doc_id, target_index, binlog_file, binlog_pos, payload, retry_count, next_retry_at, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)"
	esSyncDLQInsertSQL   = "INSERT INTO es_sync_dlq(table_name, event_type, doc_id, target_index, binlog_file, binlog_pos, payload, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	esSyncRetryUpdateSQL = "UPDATE es_sync_retry SET retry_count = ?, next_retry_at = ?, last_error = ? WHERE id = ?"
	esSyncRetryDeleteSQL = "DELETE FROM es_sync_retry WHERE id = ?"
	syncBinlogFileField  = "sync_binlog_file"
	syncBinlogPosField   = "sync_binlog_pos"
	syncEventTypeField   = "sync_event_type"
	syncAtField          = "sync_at"
)

var (
	esSyncRetryTableOnce sync.Once
	esSyncRetryTableErr  error
	binlogFileSeqRegexp  = regexp.MustCompile(`(\d+)$`)
)

type esSyncVersion struct {
	File string `json:"sync_binlog_file"`
	Pos  int64  `json:"sync_binlog_pos"`
}

type esSyncRetryPayload struct {
	Table      string                 `json:"table"`
	EventType  string                 `json:"event_type"`
	Row        map[string]interface{} `json:"row"`
	Version    esSyncVersion          `json:"version"`
	DocID      string                 `json:"doc_id"`
	Index      string                 `json:"index"`
	ReceivedAt string                 `json:"received_at"`
}

type esSyncRetryRow struct {
	ID          int64
	TableName   string
	EventType   string
	DocID       string
	TargetIndex string
	BinlogFile  string
	BinlogPos   int64
	Payload     []byte
	RetryCount  int
	LastError   sql.NullString
}

type postSearchRow struct {
	ID        int64
	PostID    int64
	AuthorID  int64
	Title     string
	Content   string
	Status    int32
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt
}

type studyCommentSearchRow struct {
	ID                 int64
	CommentID          int64
	PostID             int64
	StudentID          int64
	Content            string
	VisibleStatus      int32
	AuditStatus        int32
	ManualReviewReason *string
	ManualOperatorID   int64
	ReplyID            int64
	ReplyTutorID       int64
	ReplyContent       *string
	ReplyStatus        int32
	RepliedAt          *time.Time
	DeletedAt          gorm.DeletedAt
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

func (js *JobWorker) handleCanalKafkaMessage(ctx context.Context, m kafka.Message) error {
	msg := new(Msg)
	if err := json.Unmarshal(m.Value, msg); err != nil {
		return js.savePoisonESSyncMessage(ctx, m, "unmarshal canal json failed: "+err.Error())
	}
	if msg.IsDdl {
		return nil
	}

	version := versionFromCanalMessage(msg, m)
	js.log.Debugf("canal msg table=%s type=%s data_len=%d version=%s:%d", msg.Table, msg.Type, len(msg.Data), version.File, version.Pos)

	for _, rowData := range msg.Data {
		if err := js.processCanalRow(ctx, msg.Table, msg.Type, rowData, version); err != nil {
			if retryErr := js.saveESSyncRetry(ctx, msg.Table, msg.Type, rowData, version, err); retryErr != nil {
				return fmt.Errorf("process canal row failed: %w; save retry failed: %v", err, retryErr)
			}
			js.log.Warnf("es sync failed and saved to retry, table=%s, type=%s, err=%v", msg.Table, msg.Type, err)
		}
	}
	return nil
}

func (js *JobWorker) processCanalRow(ctx context.Context, table, eventType string, row map[string]interface{}, version esSyncVersion) error {
	docID, targetIndex, ok := getDocIDAndIndex(table, row)
	if !ok {
		return fmt.Errorf("invalid doc id, table=%s row=%+v", table, row)
	}
	if old, hit, err := js.currentESSyncVersion(ctx, targetIndex, docID); err != nil {
		return err
	} else if hit && !isNewerSyncVersion(version, old) {
		js.log.Debugf("skip old es sync message, index=%s, id=%s, incoming=%s:%d, current=%s:%d",
			targetIndex, docID, version.File, version.Pos, old.File, old.Pos)
		return nil
	}

	doc, err := js.buildLatestESDoc(ctx, table, docID, eventType)
	if err != nil {
		return err
	}
	decorateESSyncDoc(doc, eventType, version)

	return js.indexDocument(ctx, docID, doc, targetIndex)
}

func (js *JobWorker) buildLatestESDoc(ctx context.Context, table, docID, eventType string) (map[string]interface{}, error) {
	switch table {
	case "post":
		postID, _ := strconv.ParseInt(docID, 10, 64)
		var post postSearchRow
		err := js.data.DB().WithContext(ctx).Unscoped().Table("post").Where("post_id = ?", postID).Take(&post).Error
		if errors.Is(err, gorm.ErrRecordNotFound) || strings.EqualFold(eventType, "DELETE") {
			// 删除事件写 tombstone 文档而不是物理 delete。
			// tombstone 保留 sync_version，能挡住后续晚到的旧 UPDATE 重试消息。
			return map[string]interface{}{
				"post_id":    postID,
				"status":     int32(2),
				"deleted_at": time.Now().Format("2006-01-02 15:04:05"),
			}, nil
		}
		if err != nil {
			return nil, err
		}
		return buildPostESDocFromDB(post), nil

	case "study_comment":
		commentID, _ := strconv.ParseInt(docID, 10, 64)
		var comment studyCommentSearchRow
		err := js.data.DB().WithContext(ctx).Unscoped().Table("study_comment").Where("comment_id = ?", commentID).Take(&comment).Error
		if errors.Is(err, gorm.ErrRecordNotFound) || strings.EqualFold(eventType, "DELETE") {
			// 评论同样写不可见 tombstone，搜索查询侧通过 visible/audit/deleted_at 过滤。
			return map[string]interface{}{
				"comment_id":     commentID,
				"visible_status": int32(2),
				"audit_status":   int32(2),
				"deleted_at":     time.Now().Format("2006-01-02 15:04:05"),
			}, nil
		}
		if err != nil {
			return nil, err
		}
		return buildStudyCommentESDocFromDB(comment), nil

	default:
		return nil, fmt.Errorf("unsupported es sync table=%s", table)
	}
}

func buildPostESDocFromDB(post postSearchRow) map[string]interface{} {
	return map[string]interface{}{
		"id":         post.ID,
		"post_id":    post.PostID,
		"author_id":  post.AuthorID,
		"title":      post.Title,
		"content":    post.Content,
		"status":     post.Status,
		"created_at": formatESTime(post.CreatedAt),
		"updated_at": formatESTime(post.UpdatedAt),
		"deleted_at": formatGormDeletedAt(post.DeletedAt),
	}
}

func buildStudyCommentESDocFromDB(comment studyCommentSearchRow) map[string]interface{} {
	return map[string]interface{}{
		"id":                   comment.ID,
		"comment_id":           comment.CommentID,
		"post_id":              comment.PostID,
		"student_id":           comment.StudentID,
		"content":              comment.Content,
		"visible_status":       comment.VisibleStatus,
		"audit_status":         comment.AuditStatus,
		"manual_operator_id":   comment.ManualOperatorID,
		"manual_review_reason": nullableString(comment.ManualReviewReason),
		"reply_id":             comment.ReplyID,
		"reply_tutor_id":       comment.ReplyTutorID,
		"reply_content":        nullableString(comment.ReplyContent),
		"reply_status":         comment.ReplyStatus,
		"replied_at":           formatNullableESTime(comment.RepliedAt),
		"created_at":           formatESTime(comment.CreatedAt),
		"updated_at":           formatESTime(comment.UpdatedAt),
		"deleted_at":           formatGormDeletedAt(comment.DeletedAt),
	}
}

func decorateESSyncDoc(doc map[string]interface{}, eventType string, version esSyncVersion) {
	doc[syncBinlogFileField] = version.File
	doc[syncBinlogPosField] = version.Pos
	doc[syncEventTypeField] = strings.ToUpper(eventType)
	doc[syncAtField] = time.Now().Format("2006-01-02 15:04:05")
}

func (js *JobWorker) currentESSyncVersion(ctx context.Context, index, docID string) (esSyncVersion, bool, error) {
	resp, err := js.esClient.esClient.Get(index, docID).
		SourceIncludes_(syncBinlogFileField, syncBinlogPosField).
		Do(ctx)
	if err != nil {
		return esSyncVersion{}, false, err
	}
	if resp == nil || !resp.Found || len(resp.Source_) == 0 {
		return esSyncVersion{}, false, nil
	}

	var source map[string]interface{}
	if err := json.Unmarshal(resp.Source_, &source); err != nil {
		return esSyncVersion{}, false, err
	}
	file := strings.TrimSpace(toString(source[syncBinlogFileField]))
	pos := toInt64(source[syncBinlogPosField])
	if file == "" || pos <= 0 {
		return esSyncVersion{}, false, nil
	}
	return esSyncVersion{File: file, Pos: pos}, true, nil
}

func versionFromCanalMessage(msg *Msg, m kafka.Message) esSyncVersion {
	if msg != nil && strings.TrimSpace(msg.BinlogFile) != "" && msg.BinlogPos > 0 {
		return esSyncVersion{File: strings.TrimSpace(msg.BinlogFile), Pos: msg.BinlogPos}
	}
	if msg != nil && strings.TrimSpace(msg.BinlogFileCamel) != "" && msg.BinlogPosCamel > 0 {
		return esSyncVersion{File: strings.TrimSpace(msg.BinlogFileCamel), Pos: msg.BinlogPosCamel}
	}
	return esSyncVersion{
		File: fmt.Sprintf("kafka:%s:%d", m.Topic, m.Partition),
		Pos:  m.Offset,
	}
}

func isNewerSyncVersion(incoming, current esSyncVersion) bool {
	if incoming.File == current.File {
		return incoming.Pos > current.Pos
	}
	inSeq, inOK := binlogFileSeq(incoming.File)
	curSeq, curOK := binlogFileSeq(current.File)
	if inOK && curOK && inSeq != curSeq {
		return inSeq > curSeq
	}
	return incoming.File > current.File
}

func binlogFileSeq(file string) (int64, bool) {
	matches := binlogFileSeqRegexp.FindStringSubmatch(file)
	if len(matches) < 2 {
		return 0, false
	}
	n, err := strconv.ParseInt(matches[1], 10, 64)
	return n, err == nil
}

func (js *JobWorker) saveESSyncRetry(ctx context.Context, table, eventType string, row map[string]interface{}, version esSyncVersion, cause error) error {
	if js.data == nil || js.data.DB() == nil {
		return fmt.Errorf("db is not ready")
	}
	if err := ensureESSyncRetryTables(js.data.DB()); err != nil {
		return err
	}
	docID, targetIndex, ok := getDocIDAndIndex(table, row)
	if !ok {
		return js.savePoisonPayloadToDLQ(ctx, table, eventType, "", "", version, row, cause)
	}
	payload := esSyncRetryPayload{
		Table:      table,
		EventType:  eventType,
		Row:        row,
		Version:    version,
		DocID:      docID,
		Index:      targetIndex,
		ReceivedAt: time.Now().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return js.data.DB().WithContext(ctx).Exec(
		esSyncRetryInsertSQL,
		table,
		eventType,
		docID,
		targetIndex,
		version.File,
		version.Pos,
		string(data),
		time.Now().Add(esRetryBackoffBase),
		cause.Error(),
	).Error
}

func (js *JobWorker) savePoisonESSyncMessage(ctx context.Context, m kafka.Message, reason string) error {
	if js.data == nil || js.data.DB() == nil {
		return nil
	}
	version := esSyncVersion{File: fmt.Sprintf("kafka:%s:%d", m.Topic, m.Partition), Pos: m.Offset}
	row := map[string]interface{}{
		"raw": string(m.Value),
	}
	return js.savePoisonPayloadToDLQ(ctx, "unknown", "POISON", "", "", version, row, errors.New(reason))
}

func (js *JobWorker) savePoisonPayloadToDLQ(ctx context.Context, table, eventType, docID, targetIndex string, version esSyncVersion, row map[string]interface{}, cause error) error {
	if err := ensureESSyncRetryTables(js.data.DB()); err != nil {
		return err
	}
	payload, _ := json.Marshal(row)
	return js.data.DB().WithContext(ctx).Exec(
		esSyncDLQInsertSQL,
		table,
		eventType,
		docID,
		targetIndex,
		version.File,
		version.Pos,
		string(payload),
		cause.Error(),
	).Error
}

func ensureESSyncRetryTables(db *gorm.DB) error {
	esSyncRetryTableOnce.Do(func() {
		if err := db.Exec(esSyncRetryTableDDL).Error; err != nil {
			esSyncRetryTableErr = err
			return
		}
		esSyncRetryTableErr = db.Exec(esSyncDLQTableDDL).Error
	})
	return esSyncRetryTableErr
}

func (js *JobWorker) startESSyncRetryWorker(ctx context.Context) {
	if js == nil || js.data == nil || js.data.DB() == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(esRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := js.processESSyncRetries(ctx); err != nil && ctx.Err() == nil {
					js.log.Warnf("process es sync retries failed: %v", err)
				}
			}
		}
	}()
}

func (js *JobWorker) processESSyncRetries(ctx context.Context) error {
	if err := ensureESSyncRetryTables(js.data.DB()); err != nil {
		return err
	}
	var rows []esSyncRetryRow
	if err := js.data.DB().WithContext(ctx).
		Raw("SELECT id, table_name, event_type, doc_id, target_index, binlog_file, binlog_pos, payload, retry_count, last_error FROM es_sync_retry WHERE next_retry_at <= NOW() ORDER BY id ASC LIMIT ?", esRetryBatchSize).
		Scan(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		if err := js.processESSyncRetryRow(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func (js *JobWorker) processESSyncRetryRow(ctx context.Context, row esSyncRetryRow) error {
	var payload esSyncRetryPayload
	if err := json.Unmarshal(row.Payload, &payload); err != nil {
		return js.moveESSyncRetryToDLQ(ctx, row, err)
	}
	version := esSyncVersion{File: row.BinlogFile, Pos: row.BinlogPos}
	if version.File == "" || version.Pos == 0 {
		version = payload.Version
	}
	if err := js.processCanalRow(ctx, row.TableName, row.EventType, payload.Row, version); err != nil {
		nextRetryCount := row.RetryCount + 1
		if nextRetryCount >= esRetryMaxAttempts {
			return js.moveESSyncRetryToDLQ(ctx, row, err)
		}
		return js.data.DB().WithContext(ctx).Exec(
			esSyncRetryUpdateSQL,
			nextRetryCount,
			time.Now().Add(esRetryBackoffBase*time.Duration(nextRetryCount+1)),
			err.Error(),
			row.ID,
		).Error
	}
	return js.data.DB().WithContext(ctx).Exec(esSyncRetryDeleteSQL, row.ID).Error
}

func (js *JobWorker) moveESSyncRetryToDLQ(ctx context.Context, row esSyncRetryRow, cause error) error {
	return js.data.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			esSyncDLQInsertSQL,
			row.TableName,
			row.EventType,
			row.DocID,
			row.TargetIndex,
			row.BinlogFile,
			row.BinlogPos,
			string(row.Payload),
			cause.Error(),
		).Error; err != nil {
			return err
		}
		return tx.Exec(esSyncRetryDeleteSQL, row.ID).Error
	})
}

func formatESTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02 15:04:05")
}

func formatNullableESTime(t *time.Time) interface{} {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02 15:04:05")
}

func formatGormDeletedAt(t gorm.DeletedAt) interface{} {
	if !t.Valid || t.Time.IsZero() {
		return nil
	}
	return t.Time.Format("2006-01-02 15:04:05")
}

func nullableString(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}
