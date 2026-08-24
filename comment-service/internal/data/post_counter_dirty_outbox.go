package data

import (
	"context"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	counterDirtyOutboxTableDDL     = "CREATE TABLE IF NOT EXISTS counter_dirty_outbox (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, event_type VARCHAR(64) NOT NULL, post_id BIGINT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'pending', retry_count INT NOT NULL DEFAULT 0, next_retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, last_error VARCHAR(512) NOT NULL DEFAULT '', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, KEY idx_dirty_outbox_status_retry (status, next_retry_at), KEY idx_dirty_outbox_post (post_id, event_type, status))"
	counterDirtyOutboxScanInterval = time.Second
	counterDirtyOutboxBatchSize    = 100
	counterDirtyOutboxRetryDelay   = 3 * time.Second
)

type counterDirtyOutboxRow struct {
	ID        uint64
	EventType string
	PostID    int64
}

// recordCounterDirtyOutbox 在首次 dirty 时记录一条可靠通知任务。
//
// Kafka 消息只负责唤醒 comment-task；如果 Kafka 发送失败，outbox 会保留 pending 状态等待重试。
func (d *Data) recordCounterDirtyOutbox(ctx context.Context, eventType string, postID int64) error {
	if d == nil || d.db == nil || postID <= 0 {
		return nil
	}
	if err := ensureCounterDirtyOutboxTable(d.db); err != nil {
		return err
	}
	return d.db.WithContext(ctx).Exec(`INSERT INTO counter_dirty_outbox(event_type, post_id, status, retry_count, next_retry_at)
VALUES (?, ?, 'pending', 0, NOW())`, eventType, postID).Error
}

func (d *Data) startCounterDirtyOutboxPublisher(ctx context.Context) {
	if d == nil || d.db == nil || d.postLikeDirtyWriter == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(counterDirtyOutboxScanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.publishCounterDirtyOutbox(ctx, counterDirtyOutboxBatchSize); err != nil && ctx.Err() == nil {
					d.log.WithContext(ctx).Warnf("publish counter dirty outbox failed: %v", err)
				}
			}
		}
	}()
}

func (d *Data) publishCounterDirtyOutbox(ctx context.Context, batchSize int) error {
	if batchSize <= 0 {
		batchSize = counterDirtyOutboxBatchSize
	}
	if err := ensureCounterDirtyOutboxTable(d.db); err != nil {
		return err
	}

	var rows []counterDirtyOutboxRow
	if err := d.db.WithContext(ctx).Raw(`SELECT id, event_type, post_id
FROM counter_dirty_outbox
WHERE (status = 'pending' AND next_retry_at <= NOW())
   OR (status = 'processing' AND updated_at < DATE_SUB(NOW(), INTERVAL 60 SECOND))
ORDER BY id ASC
LIMIT ?`, batchSize).Scan(&rows).Error; err != nil {
		return err
	}

	for _, row := range rows {
		if row.ID == 0 || row.PostID <= 0 {
			continue
		}
		if ok, err := d.claimCounterDirtyOutbox(ctx, row.ID); err != nil {
			return err
		} else if !ok {
			continue
		}

		if err := d.postLikeDirtyWriter.notify(ctx, row.EventType, row.PostID); err != nil {
			d.markCounterDirtyOutboxPending(ctx, row.ID, err)
			continue
		}
		if err := d.db.WithContext(ctx).Exec(`UPDATE counter_dirty_outbox
SET status = 'sent', last_error = '', updated_at = NOW()
WHERE id = ?`, row.ID).Error; err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) claimCounterDirtyOutbox(ctx context.Context, id uint64) (bool, error) {
	result := d.db.WithContext(ctx).Exec(`UPDATE counter_dirty_outbox
SET status = 'processing', updated_at = NOW()
WHERE id = ?
  AND (status = 'pending' OR (status = 'processing' AND updated_at < DATE_SUB(NOW(), INTERVAL 60 SECOND)))`, id)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (d *Data) markCounterDirtyOutboxPending(ctx context.Context, id uint64, cause error) {
	msg := ""
	if cause != nil {
		msg = cause.Error()
		if len(msg) > 512 {
			msg = msg[:512]
		}
	}
	nextRetryAt := time.Now().Add(counterDirtyOutboxRetryDelay)
	if err := d.db.WithContext(ctx).Exec(`UPDATE counter_dirty_outbox
SET status = 'pending',
    retry_count = retry_count + 1,
    next_retry_at = ?,
    last_error = ?,
    updated_at = NOW()
WHERE id = ?`, nextRetryAt, msg, id).Error; err != nil {
		d.log.WithContext(ctx).Warnf("mark counter dirty outbox pending failed, id=%d, err=%v", id, err)
	}
}

var (
	counterDirtyOutboxTableOnce sync.Once
	counterDirtyOutboxTableErr  error
)

func ensureCounterDirtyOutboxTable(db *gorm.DB) error {
	counterDirtyOutboxTableOnce.Do(func() {
		counterDirtyOutboxTableErr = db.Exec(counterDirtyOutboxTableDDL).Error
	})
	return counterDirtyOutboxTableErr
}
