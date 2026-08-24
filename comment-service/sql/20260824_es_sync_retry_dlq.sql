-- ES 同步失败重试表和死信表。
--
-- comment-task 消费 Canal/Kafka 消息时，只有 ES 写成功，或者失败消息已可靠写入
-- es_sync_retry/es_sync_dlq 后，才提交原 Kafka offset，避免搜索同步消息静默丢失。

CREATE TABLE IF NOT EXISTS es_sync_retry (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL COMMENT '来源表名',
    event_type VARCHAR(16) NOT NULL COMMENT 'Canal事件类型',
    doc_id VARCHAR(64) NOT NULL COMMENT 'ES文档ID',
    target_index VARCHAR(64) NOT NULL COMMENT 'ES索引名',
    binlog_file VARCHAR(128) NOT NULL COMMENT 'MySQL binlog文件',
    binlog_pos BIGINT NOT NULL COMMENT 'MySQL binlog位点',
    payload JSON NOT NULL COMMENT '重试所需原始payload',
    retry_count INT NOT NULL DEFAULT 0 COMMENT '已重试次数',
    next_retry_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '下次允许重试时间',
    last_error TEXT NULL COMMENT '最近一次失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_es_sync_retry_due (next_retry_at),
    KEY idx_es_sync_retry_doc (target_index, doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ES同步失败重试表';

CREATE TABLE IF NOT EXISTS es_sync_dlq (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL COMMENT '来源表名',
    event_type VARCHAR(16) NOT NULL COMMENT 'Canal事件类型',
    doc_id VARCHAR(64) NOT NULL COMMENT 'ES文档ID',
    target_index VARCHAR(64) NOT NULL COMMENT 'ES索引名',
    binlog_file VARCHAR(128) NOT NULL COMMENT 'MySQL binlog文件',
    binlog_pos BIGINT NOT NULL COMMENT 'MySQL binlog位点',
    payload JSON NOT NULL COMMENT '死信原始payload',
    last_error TEXT NULL COMMENT '失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ES同步死信表';
