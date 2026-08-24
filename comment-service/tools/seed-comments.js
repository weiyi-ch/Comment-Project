#!/usr/bin/env node
'use strict';

const DEFAULT_COMMENT_TEMPLATES = [
  '老师讲得太好啦，打卡学习！',
  '老师，精听遇到听不懂的长难句怎么办？',
  '已学',
  'k6 load test comment 0 from vu 1',
  'k6 load test comment 1 from vu 1',
];

const env = process.env;

const BASE_URL = stripTrailingSlash(env.BASE_URL || 'http://127.0.0.1:8888');
const POST_IDS = parseList(env.POST_IDS || env.POST_ID || '');
const STUDENT_START = parsePositiveInt(env.STUDENT_START, 200000);
const REPEAT_PER_POST = parsePositiveInt(env.REPEAT_PER_POST, 1);
const CONCURRENCY = parsePositiveInt(env.CONCURRENCY, 5);
const REQUEST_ID_PREFIX = env.REQUEST_ID_PREFIX || 'seed-comments';
const DRY_RUN = parseBool(env.DRY_RUN);
const LOG_RESPONSE = parseBool(env.LOG_RESPONSE);
const UNIQUE_SUFFIX = parseBool(env.UNIQUE_SUFFIX);
const COMMENT_CONTENTS = parseContents(env.COMMENT_CONTENTS || '');

main().catch((err) => {
  console.error(`[seed-comments] failed: ${err.stack || err.message || err}`);
  process.exit(1);
});

async function main() {
  if (typeof fetch !== 'function') {
    throw new Error('Node.js fetch API is not available. Please use Node.js 18+.');
  }
  if (POST_IDS.length === 0) {
    throw new Error('POST_IDS is required, for example: POST_IDS="$POST_IDS" node tools/seed-comments.js');
  }
  if (COMMENT_CONTENTS.length === 0) {
    throw new Error('COMMENT_CONTENTS is empty');
  }

  const tasks = buildTasks(POST_IDS, COMMENT_CONTENTS, REPEAT_PER_POST);

  console.log('[seed-comments] config');
  console.log(`base_url=${BASE_URL}`);
  console.log(`posts=${POST_IDS.length}`);
  console.log(`templates=${COMMENT_CONTENTS.length}`);
  console.log(`repeat_per_post=${REPEAT_PER_POST}`);
  console.log(`total_comments=${tasks.length}`);
  console.log(`student_start=${STUDENT_START}`);
  console.log(`concurrency=${CONCURRENCY}`);
  console.log(`dry_run=${DRY_RUN}`);

  if (DRY_RUN) {
    for (const task of tasks.slice(0, 10)) {
      console.log(`[dry-run] post_id=${task.postID} student_id=${task.studentID} content=${task.content}`);
    }
    if (tasks.length > 10) {
      console.log(`[dry-run] ... ${tasks.length - 10} more tasks`);
    }
    return;
  }

  let cursor = 0;
  let success = 0;
  let failed = 0;
  const failures = [];
  const workerCount = Math.min(CONCURRENCY, tasks.length);

  await Promise.all(Array.from({ length: workerCount }, async () => {
    for (;;) {
      const task = tasks[cursor++];
      if (!task) {
        return;
      }

      try {
        const result = await createComment(task);
        success++;
        if (LOG_RESPONSE) {
          console.log(`[ok] post_id=${task.postID} student_id=${task.studentID} status=${result.status} body=${truncate(result.body, 300)}`);
        }
      } catch (err) {
        failed++;
        failures.push({ task, error: err });
        console.error(`[error] post_id=${task.postID} student_id=${task.studentID} ${err.message}`);
      }
    }
  }));

  console.log('[seed-comments] summary');
  console.log(`success=${success}`);
  console.log(`failed=${failed}`);
  console.log(`total=${tasks.length}`);

  if (failures.length > 0) {
    console.log('[seed-comments] first failures');
    for (const item of failures.slice(0, 10)) {
      console.log(`post_id=${item.task.postID} student_id=${item.task.studentID} error=${item.error.message}`);
    }
    process.exit(1);
  }
}

function buildTasks(postIDs, contents, repeatPerPost) {
  const tasks = [];
  let seq = 0;

  for (const postID of postIDs) {
    for (let round = 0; round < repeatPerPost; round++) {
      for (let contentIndex = 0; contentIndex < contents.length; contentIndex++) {
        const baseContent = contents[contentIndex];
        const suffix = UNIQUE_SUFFIX ? ` #seed-${round + 1}-${contentIndex + 1}` : '';
        tasks.push({
          seq,
          postID,
          studentID: STUDENT_START + seq,
          content: `${baseContent}${suffix}`,
        });
        seq++;
      }
    }
  }

  return tasks;
}

async function createComment(task) {
  const url = `${BASE_URL}/v1/student/posts/${encodeURIComponent(task.postID)}/comments`;
  const requestID = `${REQUEST_ID_PREFIX}-${task.seq}`;
  const body = JSON.stringify({
    student_id: String(task.studentID),
    post_id: String(task.postID),
    content: task.content,
  });

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-request-id': requestID,
    },
    body,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`status=${res.status} request_id=${requestID} body=${truncate(text, 500)}`);
  }

  return { status: res.status, body: text };
}

function parseContents(raw) {
  if (!raw.trim()) {
    return DEFAULT_COMMENT_TEMPLATES;
  }

  const separator = env.COMMENT_CONTENT_SEPARATOR || '||';
  return raw.split(separator).map((item) => item.trim()).filter(Boolean);
}

function parseList(raw) {
  return String(raw || '')
    .split(/[,\s]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parsePositiveInt(raw, fallback) {
  const value = Number.parseInt(raw || '', 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function parseBool(raw) {
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(raw || '').toLowerCase());
}

function stripTrailingSlash(value) {
  return String(value).replace(/\/$/, '');
}

function truncate(value, maxLength) {
  const text = String(value || '');
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}
