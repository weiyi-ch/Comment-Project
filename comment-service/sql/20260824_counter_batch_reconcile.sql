-- 计数异步落库可靠批次表和校准日志表。
--
-- counter_batch 记录 pending -> processing_batch 后的可靠 MySQL 批次；
-- counter_dirty_outbox 记录首次 dirty 的 Kafka 唤醒通知，发送失败后可重试；
-- counter_reconcile_log 记录定期校准任务的修复或跳过原因，方便排查计数漂移。

CREATE TABLE IF NOT EXISTS counter_batch (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(128) NOT NULL COMMENT '计数批次ID，保证异步落库幂等',
    counter_type VARCHAR(32) NOT NULL COMMENT 'post_like/post_comment',
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    delta BIGINT NOT NULL COMMENT '本批次计数增量',
    status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT 'pending/processing/success/failed',
    retry_count INT NOT NULL DEFAULT 0 COMMENT '重试次数',
    next_retry_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '下次允许重试时间',
    last_error VARCHAR(512) NOT NULL DEFAULT '' COMMENT '最近一次失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_counter_batch_id (batch_id),
    KEY idx_counter_batch_status_retry (status, next_retry_at),
    KEY idx_counter_batch_post_status (post_id, counter_type, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计数异步落库批次表';

CREATE TABLE IF NOT EXISTS counter_dirty_outbox (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL COMMENT 'Kafka dirty事件类型',
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT 'pending/processing/sent',
    retry_count INT NOT NULL DEFAULT 0 COMMENT '重试次数',
    next_retry_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '下次允许重试时间',
    last_error VARCHAR(512) NOT NULL DEFAULT '' COMMENT '最近一次失败原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_dirty_outbox_status_retry (status, next_retry_at),
    KEY idx_dirty_outbox_post (post_id, event_type, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计数dirty通知outbox表';

CREATE TABLE IF NOT EXISTS counter_reconcile_log (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    counter_type VARCHAR(32) NOT NULL COMMENT 'post_like/post_comment',
    old_count BIGINT NOT NULL COMMENT '校准前post_counter值',
    fact_count BIGINT NOT NULL COMMENT '事实表重新聚合值',
    diff BIGINT NOT NULL COMMENT 'fact_count - old_count',
    status VARCHAR(32) NOT NULL COMMENT 'fixed/skipped',
    reason VARCHAR(255) NOT NULL DEFAULT '' COMMENT '跳过或修复原因',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_post_type_time (post_id, counter_type, created_at),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='计数校准日志表';
