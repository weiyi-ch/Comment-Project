package task

import (
	"errors"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/segmentio/kafka-go"
	canalentry "github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"
)

func TestExternalESSyncVersionFromBinlogPosition(t *testing.T) {
	version, ok := externalESSyncVersion(esSyncVersion{
		File: "mysql-bin.000123",
		Pos:  456789,
	})
	if !ok {
		t.Fatal("binlog version should be converted")
	}

	want := "123000000456789"
	if version != want {
		t.Fatalf("unexpected external version, got=%s want=%s", version, want)
	}
}

func TestExternalESSyncVersionFromKafkaFallback(t *testing.T) {
	version, ok := externalESSyncVersion(esSyncVersion{
		File: "kafka:comment-service:0",
		Pos:  12,
	})
	if !ok {
		t.Fatal("kafka fallback version should be converted")
	}

	if version != "13" {
		t.Fatalf("unexpected kafka fallback version, got=%s", version)
	}
}

func TestExternalESSyncVersionRejectsOverflow(t *testing.T) {
	_, ok := externalESSyncVersion(esSyncVersion{
		File: "mysql-bin.999999999",
		Pos:  1,
	})
	if ok {
		t.Fatal("overflowed external version should be rejected")
	}
}

func TestIsESVersionConflict(t *testing.T) {
	err := &types.ElasticsearchError{Status: 409}
	if !isESVersionConflict(err) {
		t.Fatal("409 elasticsearch error should be treated as version conflict")
	}
	if isESVersionConflict(errors.New("status: 409")) {
		t.Fatal("plain error should not be treated as version conflict")
	}
}

func TestParseEntryCanalMessage(t *testing.T) {
	rowChange := &canalentry.RowChange{
		EventTypePresent: &canalentry.RowChange_EventType{EventType: canalentry.EventType_UPDATE},
		RowDatas: []*canalentry.RowData{
			{
				AfterColumns: []*canalentry.Column{
					{Name: "post_id", Value: "1001"},
					{Name: "author_id", Value: "2001"},
					{Name: "title", Value: "entry title"},
					{Name: "content", Value: "entry content"},
					{Name: "status", Value: "1"},
					{Name: "created_at", Value: "2026-09-10 10:00:00"},
					{Name: "updated_at", Value: "2026-09-10 10:01:00"},
					{Name: "deleted_at", IsNullPresent: &canalentry.Column_IsNull{IsNull: true}},
				},
			},
		},
	}
	storeValue, err := proto.Marshal(rowChange)
	if err != nil {
		t.Fatal(err)
	}
	entry := &canalentry.Entry{
		Header: &canalentry.Header{
			LogfileName:   "mysql-bin.000003",
			LogfileOffset: 1024,
			SchemaName:    "comment",
			TableName:     "post",
		},
		EntryTypePresent: &canalentry.Entry_EntryType{EntryType: canalentry.EntryType_ROWDATA},
		StoreValue:       storeValue,
	}
	value, err := proto.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}

	events, err := parseCanalKafkaMessage(kafka.Message{Topic: "post", Partition: 0, Offset: 9, Value: value})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("unexpected events length, got=%d", len(events))
	}

	event := events[0]
	if event.Table != "post" || event.EventType != "UPDATE" {
		t.Fatalf("unexpected event, got table=%s type=%s", event.Table, event.EventType)
	}
	if event.Version.File != "mysql-bin.000003" || event.Version.Pos != 1024 {
		t.Fatalf("unexpected version, got=%s:%d", event.Version.File, event.Version.Pos)
	}

	doc, err := buildESDocFromCanalRow(event.Table, event.EventType, event.Row)
	if err != nil {
		t.Fatal(err)
	}
	if doc["post_id"] != int64(1001) || doc["title"] != "entry title" || doc["deleted_at"] != nil {
		t.Fatalf("unexpected doc=%+v", doc)
	}
}
