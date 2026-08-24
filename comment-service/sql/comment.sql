CREATE DATABASE IF NOT EXISTS comment DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE comment;

CREATE TABLE IF NOT EXISTS user_account (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL COMMENT '业务用户ID',
    username VARCHAR(64) NOT NULL COMMENT '登录用户名',
    role VARCHAR(32) NOT NULL COMMENT 'student/tutor/operator',
    password_hash VARCHAR(128) NOT NULL COMMENT 'bcrypt密码哈希',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1启用 2禁用',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_id (user_id),
    UNIQUE KEY uk_role_username (role, username),
    KEY idx_role_status_ct (role, status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户账号表';

CREATE TABLE IF NOT EXISTS refresh_token_session (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    token_id VARCHAR(64) NOT NULL COMMENT 'refresh token ID',
    user_id BIGINT NOT NULL COMMENT '业务用户ID',
    role VARCHAR(32) NOT NULL COMMENT 'student/tutor/operator',
    token_hash CHAR(64) NOT NULL COMMENT 'refresh token secret SHA-256',
    expires_at DATETIME NOT NULL COMMENT '过期时间',
    revoked TINYINT NOT NULL DEFAULT 0 COMMENT '0有效 1已撤销',
    replaced_by VARCHAR(64) DEFAULT NULL COMMENT '轮换后的新token_id',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_token_id (token_id),
    KEY idx_user_role_revoked (user_id, role, revoked),
    KEY idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='刷新令牌会话表';

CREATE TABLE IF NOT EXISTS post (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    author_id BIGINT NOT NULL COMMENT '助教ID',
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1已发布 2已下架/删除',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL,
    UNIQUE KEY uk_post_id (post_id),
    KEY idx_author_status_ct (author_id, status, created_at),
    KEY idx_status_ct (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识帖表';

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

CREATE TABLE IF NOT EXISTS post_like (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    student_id BIGINT NOT NULL COMMENT '学生ID',
    status TINYINT NOT NULL DEFAULT 1 COMMENT '1已点赞 0已取消',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_post_student (post_id, student_id),
    KEY idx_student_ct (student_id, created_at),
    KEY idx_post_status_ct (post_id, status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='知识帖点赞表';

CREATE TABLE IF NOT EXISTS study_comment (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    comment_id BIGINT NOT NULL COMMENT '学生评论ID',
    post_id BIGINT NOT NULL COMMENT '知识帖ID',
    student_id BIGINT NOT NULL COMMENT '评论学生ID',
    content VARCHAR(1000) NOT NULL COMMENT '评论内容',
    visible_status TINYINT NOT NULL DEFAULT 1 COMMENT '1可见 2不可见',
    audit_status TINYINT NOT NULL DEFAULT 0 COMMENT '0待审核 1审核通过 2审核驳回',
    manual_review_reason VARCHAR(255) DEFAULT NULL COMMENT '人工驳回原因',
    manual_operator_id BIGINT NOT NULL DEFAULT 0 COMMENT '运营ID',
    reply_id BIGINT NOT NULL DEFAULT 0 COMMENT '助教回复ID，0表示未回复',
    reply_tutor_id BIGINT NOT NULL DEFAULT 0 COMMENT '回复助教ID',
    reply_content VARCHAR(1000) DEFAULT NULL COMMENT '助教一次性指导回复',
    reply_status TINYINT NOT NULL DEFAULT 0 COMMENT '0未回复 1已回复 2已撤回',
    replied_at DATETIME DEFAULT NULL COMMENT '回复时间',
    deleted_at DATETIME DEFAULT NULL COMMENT '删除时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_comment_id (comment_id),
    KEY idx_post_visible_ct (post_id, visible_status, created_at),
    KEY idx_post_audit_ct (post_id, audit_status, created_at),
    KEY idx_student_ct (student_id, created_at),
    KEY idx_visible_ct (visible_status, created_at),
    KEY idx_operator_ct (manual_operator_id, created_at),
    KEY idx_audit_status_ct (audit_status, created_at),
    KEY idx_reply_id (reply_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='学生学习打卡评论表';
