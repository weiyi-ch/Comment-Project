-- 将高频变化的帖子计数字段从 post 拆到 post_counter。
--
-- 设计目的：
-- 1. post 只保留标题、正文、状态等低频核心字段，方便 Canal 监听后同步 ES；
-- 2. like_count/comment_count 进入独立 post_counter，避免热点点赞/评论导致 post 表 binlog 高频 update；
-- 3. post_like 继续作为点赞事实表，post_counter 只是可异步重建的冗余统计。

CREATE TABLE IF NOT EXISTS post_counter (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    like_count INT NOT NULL DEFAULT 0 COMMENT '点赞数',
    comment_count INT NOT NULL DEFAULT 0 COMMENT '有效评论数',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_post_id (post_id),
    KEY idx_like_count (like_count),
    KEY idx_comment_count (comment_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识帖计数表';

INSERT INTO post_counter (post_id, like_count, comment_count)
SELECT post_id, like_count, comment_count
FROM post
ON DUPLICATE KEY UPDATE
    like_count = VALUES(like_count),
    comment_count = VALUES(comment_count);

ALTER TABLE post
    DROP COLUMN like_count,
    DROP COLUMN comment_count;
