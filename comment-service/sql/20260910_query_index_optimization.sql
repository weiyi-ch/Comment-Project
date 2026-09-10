USE comment;

-- Existing local databases can apply this migration after pulling the latest code.
-- 新库会直接使用 comment.sql 中的完整索引；已有本地库可执行本迁移补齐查询索引。

DELIMITER //

CREATE PROCEDURE add_index_if_missing(
    IN p_table_name VARCHAR(64),
    IN p_index_name VARCHAR(64),
    IN p_index_ddl TEXT
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = p_table_name
          AND index_name = p_index_name
    ) THEN
        SET @ddl = p_index_ddl;
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END//

DELIMITER ;

CALL add_index_if_missing('post', 'idx_author_status_deleted_ct',
    'ALTER TABLE post ADD KEY idx_author_status_deleted_ct (author_id, status, deleted_at, created_at)');

CALL add_index_if_missing('study_comment', 'idx_post_visible_deleted_ct',
    'ALTER TABLE study_comment ADD KEY idx_post_visible_deleted_ct (post_id, visible_status, deleted_at, created_at)');

CALL add_index_if_missing('study_comment', 'idx_student_deleted_ct',
    'ALTER TABLE study_comment ADD KEY idx_student_deleted_ct (student_id, deleted_at, created_at)');

CALL add_index_if_missing('study_comment', 'idx_audit_deleted_ct',
    'ALTER TABLE study_comment ADD KEY idx_audit_deleted_ct (audit_status, deleted_at, created_at)');

CALL add_index_if_missing('study_comment', 'idx_reply_status_deleted',
    'ALTER TABLE study_comment ADD KEY idx_reply_status_deleted (reply_id, reply_status, deleted_at)');

CALL add_index_if_missing('counter_batch', 'idx_counter_batch_type_status_retry_id',
    'ALTER TABLE counter_batch ADD KEY idx_counter_batch_type_status_retry_id (counter_type, status, next_retry_at, id)');

CALL add_index_if_missing('counter_dirty_outbox', 'idx_dirty_outbox_status_retry_id',
    'ALTER TABLE counter_dirty_outbox ADD KEY idx_dirty_outbox_status_retry_id (status, next_retry_at, id)');

CALL add_index_if_missing('counter_dirty_outbox', 'idx_dirty_outbox_status_updated_id',
    'ALTER TABLE counter_dirty_outbox ADD KEY idx_dirty_outbox_status_updated_id (status, updated_at, id)');

CALL add_index_if_missing('es_sync_retry', 'idx_es_sync_retry_due_id',
    'ALTER TABLE es_sync_retry ADD KEY idx_es_sync_retry_due_id (next_retry_at, id)');

DROP PROCEDURE add_index_if_missing;
