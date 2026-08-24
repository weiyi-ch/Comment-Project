-- 将“一条评论最多一条助教回复”内嵌到 study_comment。
-- 执行前确认旧表中每个 comment_id 最多一条记录。
SELECT comment_id, COUNT(*) AS reply_count
FROM study_comment_reply
GROUP BY comment_id
HAVING COUNT(*) > 1;

ALTER TABLE study_comment
    ADD COLUMN reply_id BIGINT NOT NULL DEFAULT 0 COMMENT '助教回复ID，0表示未回复' AFTER manual_operator_id,
    ADD COLUMN reply_tutor_id BIGINT NOT NULL DEFAULT 0 COMMENT '回复助教ID' AFTER reply_id,
    ADD COLUMN reply_content VARCHAR(1000) DEFAULT NULL COMMENT '助教一次性指导回复' AFTER reply_tutor_id,
    ADD COLUMN reply_status TINYINT NOT NULL DEFAULT 0 COMMENT '0未回复 1已回复 2已撤回' AFTER reply_content,
    ADD COLUMN replied_at DATETIME DEFAULT NULL COMMENT '回复时间' AFTER reply_status,
    ADD KEY idx_reply_id (reply_id);

UPDATE study_comment c
JOIN study_comment_reply r ON r.comment_id = c.comment_id
SET c.reply_id = r.comment_reply_id,
    c.reply_tutor_id = r.tutor_id,
    c.reply_content = r.content,
    c.reply_status = CASE WHEN r.status = 1 AND r.deleted_at IS NULL THEN 1 ELSE 2 END,
    c.replied_at = r.created_at;

DROP TABLE study_comment_reply;
