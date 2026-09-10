package task

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/segmentio/kafka-go"
	canalprotocol "github.com/withlin/canal-go/protocol"
	canalentry "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

const (
	esRetryMaxAttempts   = 3
	esRetryBatchSize     = 50
	esRetryInterval      = 5 * time.Second
	esRetryBackoffBase   = 5 * time.Second
	esSyncRetryTableDDL  = "CREATE TABLE IF NOT EXISTS es_sync_retry (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, event_type VARCHAR(16) NOT NULL, doc_id VARCHAR(64) NOT NULL, target_index VARCHAR(64) NOT NULL, binlog_file VARCHAR(128) NOT NULL, binlog_pos BIGINT NOT NULL, payload JSON NOT NULL, retry_count INT NOT NULL DEFAULT 0, next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_es_sync_retry_due (next_retry_at), KEY idx_es_sync_retry_due_id (next_retry_at, id), KEY idx_es_sync_retry_doc (target_index, doc_id))"
	esSyncDLQTableDDL    = "CREATE TABLE IF NOT EXISTS es_sync_dlq (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, table_name VARCHAR(64) NOT NULL, event_type VARCHAR(16) NOT NULL, doc_id VARCHAR(64) NOT NULL, target_index VARCHAR(64) NOT NULL, binlog_file VARCHAR(128) NOT NULL, binlog_pos BIGINT NOT NULL, payload JSON NOT NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
	esSyncRetryInsertSQL = "INSERT INTO es_sync_retry(table_name, event_type, doc_id, target_index, binlog_file, binlog_pos, payload, retry_count, next_retry_at, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)"
	esSyncDLQInsertSQL   = "INSERT INTO es_sync_dlq(table_name, event_type, doc_id, target_index, binlog_file, binlog_pos, payload, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	esSyncRetryUpdateSQL = "UPDATE es_sync_retry SET retry_count = ?, next_retry_at = ?, last_error = ? WHERE id = ?"
	esSyncRetryDeleteSQL = "DELETE FROM es_sync_retry WHERE id = ?"
	syncBinlogFileField  = "sync_binlog_file"
	syncBinlogPosField   = "sync_binlog_pos"
	syncEventTypeField   = "sync_event_type"
	syncAtField          = "sync_at"

	// ES external version 必须是单调递增 long。
	// 默认 MySQL binlog 单文件大小远小于 1e12，因此用 file 序号作为高位、pos 作为低位。
	esExternalVersionFactor = int64(1_000_000_000_000)
	maxInt64                = int64(1<<63 - 1)
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

type canalRowEvent struct {
	Database  string
	Table     string
	EventType string
	Row       map[string]interface{}
	Version   esSyncVersion
}

func (js *JobWorker) handleCanalKafkaMessage(ctx context.Context, m kafka.Message) error {
	events, err := parseCanalKafkaMessage(m)
	if err != nil {
		return js.savePoisonESSyncMessage(ctx, m, "parse canal message failed: "+err.Error())
	}

	for _, event := range events {
		js.log.Debugf("canal msg table=%s type=%s version=%s:%d", event.Table, event.EventType, event.Version.File, event.Version.Pos)
		if err := js.processCanalRow(ctx, event.Table, event.EventType, event.Row, event.Version); err != nil {
			if retryErr := js.saveESSyncRetry(ctx, event.Table, event.EventType, event.Row, event.Version, err); retryErr != nil {
				return fmt.Errorf("process canal row failed: %w; save retry failed: %v", err, retryErr)
			}
			js.log.Warnf("es sync failed and saved to retry, table=%s, type=%s, err=%v", event.Table, event.EventType, err)
		}
	}
	return nil
}

func parseCanalKafkaMessage(m kafka.Message) ([]canalRowEvent, error) {
	if looksLikeJSON(m.Value) {
		return parseFlatCanalMessage(m)
	}
	return parseEntryCanalMessage(m)
}

func looksLikeJSON(data []byte) bool {
	return bytes.HasPrefix(bytes.TrimSpace(data), []byte("{"))
}

func parseFlatCanalMessage(m kafka.Message) ([]canalRowEvent, error) {
	decoder := json.NewDecoder(bytes.NewReader(m.Value))
	decoder.UseNumber()

	msg := new(Msg)
	if err := decoder.Decode(msg); err != nil {
		return nil, err
	}
	if msg.IsDdl {
		return nil, nil
	}

	version := versionFromCanalMessage(msg, m)
	events := make([]canalRowEvent, 0, len(msg.Data))
	for _, row := range msg.Data {
		events = append(events, canalRowEvent{
			Database:  msg.Database,
			Table:     msg.Table,
			EventType: strings.ToUpper(msg.Type),
			Row:       row,
			Version:   version,
		})
	}
	return events, nil
}

func parseEntryCanalMessage(m kafka.Message) ([]canalRowEvent, error) {
	message, err := decodeCanalPacket(m.Value)
	if err == nil && message != nil {
		return eventsFromCanalEntries(message.Entries, m)
	}

	var entry canalentry.Entry
	if directErr := proto.Unmarshal(m.Value, &entry); directErr != nil {
		return nil, fmt.Errorf("decode packet failed: %v; decode entry failed: %w", err, directErr)
	}
	return eventsFromCanalEntries([]canalentry.Entry{entry}, m)
}

func decodeCanalPacket(data []byte) (message *canalprotocol.Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while decoding canal packet: %v", r)
		}
	}()
	return canalprotocol.Decode(data, false)
}

func eventsFromCanalEntries(entries []canalentry.Entry, m kafka.Message) ([]canalRowEvent, error) {
	events := make([]canalRowEvent, 0)
	for i := range entries {
		entry := entries[i]
		if entry.GetEntryType() != canalentry.EntryType_ROWDATA {
			continue
		}

		header := entry.GetHeader()
		if header == nil {
			return nil, errors.New("canal entry header is empty")
		}

		var rowChange canalentry.RowChange
		if err := proto.Unmarshal(entry.GetStoreValue(), &rowChange); err != nil {
			return nil, err
		}
		if rowChange.GetIsDdl() {
			continue
		}

		table := header.GetTableName()
		eventType := strings.ToUpper(rowChange.GetEventType().String())
		version := versionFromEntryHeader(header, m)

		for _, rowData := range rowChange.GetRowDatas() {
			row := rowDataColumns(rowData, eventType)
			events = append(events, canalRowEvent{
				Database:  header.GetSchemaName(),
				Table:     table,
				EventType: eventType,
				Row:       row,
				Version:   version,
			})
		}
	}
	return events, nil
}

func versionFromEntryHeader(header *canalentry.Header, m kafka.Message) esSyncVersion {
	if header != nil && strings.TrimSpace(header.GetLogfileName()) != "" && header.GetLogfileOffset() > 0 {
		return esSyncVersion{
			File: strings.TrimSpace(header.GetLogfileName()),
			Pos:  header.GetLogfileOffset(),
		}
	}
	return esSyncVersion{
		File: fmt.Sprintf("kafka:%s:%d", m.Topic, m.Partition),
		Pos:  m.Offset,
	}
}

func rowDataColumns(rowData *canalentry.RowData, eventType string) map[string]interface{} {
	if rowData == nil {
		return map[string]interface{}{}
	}
	if strings.EqualFold(eventType, "DELETE") {
		return columnsToMap(rowData.GetBeforeColumns())
	}
	if len(rowData.GetAfterColumns()) > 0 {
		return columnsToMap(rowData.GetAfterColumns())
	}
	return columnsToMap(rowData.GetBeforeColumns())
}

func columnsToMap(columns []*canalentry.Column) map[string]interface{} {
	row := make(map[string]interface{}, len(columns))
	for _, column := range columns {
		if column == nil {
			continue
		}
		if column.GetIsNull() {
			row[column.GetName()] = nil
			continue
		}
		row[column.GetName()] = column.GetValue()
	}
	return row
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

	doc, err := buildESDocFromCanalRow(table, eventType, row)
	if err != nil {
		return err
	}
	decorateESSyncDoc(doc, eventType, version)

	return js.indexDocument(ctx, docID, doc, targetIndex, version)
}

func buildESDocFromCanalRow(table, eventType string, row map[string]interface{}) (map[string]interface{}, error) {
	switch table {
	case "post":
		return buildPostESDocFromCanalRow(row, eventType), nil

	case "study_comment":
		return buildStudyCommentESDocFromCanalRow(row, eventType), nil

	default:
		return nil, fmt.Errorf("unsupported es sync table=%s", table)
	}
}

func buildPostESDocFromCanalRow(row map[string]interface{}, eventType string) map[string]interface{} {
	doc := map[string]interface{}{
		"id":         toInt64(row["id"]),
		"post_id":    toInt64(row["post_id"]),
		"author_id":  toInt64(row["author_id"]),
		"title":      nullableCanalString(row["title"]),
		"content":    nullableCanalString(row["content"]),
		"status":     toInt64(row["status"]),
		"created_at": nullableCanalTime(row["created_at"]),
		"updated_at": nullableCanalTime(row["updated_at"]),
		"deleted_at": nullableCanalTime(row["deleted_at"]),
	}
	if strings.EqualFold(eventType, "DELETE") {
		doc["status"] = int64(2)
		doc["deleted_at"] = time.Now().Format("2006-01-02 15:04:05")
	}
	return doc
}

func buildStudyCommentESDocFromCanalRow(row map[string]interface{}, eventType string) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                   toInt64(row["id"]),
		"comment_id":           toInt64(row["comment_id"]),
		"post_id":              toInt64(row["post_id"]),
		"student_id":           toInt64(row["student_id"]),
		"content":              nullableCanalString(row["content"]),
		"visible_status":       toInt64(row["visible_status"]),
		"audit_status":         toInt64(row["audit_status"]),
		"manual_operator_id":   toInt64(row["manual_operator_id"]),
		"manual_review_reason": nullableCanalString(row["manual_review_reason"]),
		"reply_id":             toInt64(row["reply_id"]),
		"reply_tutor_id":       toInt64(row["reply_tutor_id"]),
		"reply_content":        nullableCanalString(row["reply_content"]),
		"reply_status":         toInt64(row["reply_status"]),
		"replied_at":           nullableCanalTime(row["replied_at"]),
		"created_at":           nullableCanalTime(row["created_at"]),
		"updated_at":           nullableCanalTime(row["updated_at"]),
		"deleted_at":           nullableCanalTime(row["deleted_at"]),
	}
	if strings.EqualFold(eventType, "DELETE") {
		doc["visible_status"] = int64(2)
		doc["audit_status"] = int64(2)
		doc["deleted_at"] = time.Now().Format("2006-01-02 15:04:05")
	}
	return doc
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

func externalESSyncVersion(version esSyncVersion) (string, bool) {
	if strings.HasPrefix(version.File, "kafka:") {
		if version.Pos < 0 || version.Pos == maxInt64 {
			return "", false
		}
		return strconv.FormatInt(version.Pos+1, 10), true
	}

	seq, ok := binlogFileSeq(version.File)
	if !ok || seq < 0 || version.Pos < 0 {
		return "", false
	}
	if seq > (maxInt64-version.Pos)/esExternalVersionFactor {
		return "", false
	}
	return strconv.FormatInt(seq*esExternalVersionFactor+version.Pos, 10), true
}

func isESVersionConflict(err error) bool {
	var esErr *types.ElasticsearchError
	return errors.As(err, &esErr) && esErr.Status == 409
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
		"raw_base64": base64.StdEncoding.EncodeToString(m.Value),
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

func nullableString(s *string) interface{} {
	if s == nil {
		return nil
	}
	return *s
}

func nullableCanalString(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	s := strings.TrimSpace(toString(v))
	if s == "" || strings.EqualFold(s, "null") {
		return nil
	}
	return s
}

func nullableCanalTime(v interface{}) interface{} {
	return nullableCanalString(v)
}
