import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';

const BASE_URL = (__ENV.BASE_URL || 'http://127.0.0.1:8888').replace(/\/$/, '');
const SCENARIO = __ENV.SCENARIO || 'student-post-detail';
const POST_ID = __ENV.POST_ID || '10001';
const POST_IDS = parseList(__ENV.POST_IDS || POST_ID);
const POST_ID_MODE = (__ENV.POST_ID_MODE || 'round_robin').toLowerCase();
const HOT_TRAFFIC_PERCENT = clampPercent(Number(__ENV.HOT_TRAFFIC_PERCENT || '80'), 80);
const HOT_POST_PERCENT = clampPercent(Number(__ENV.HOT_POST_PERCENT || '20'), 20);
const HOT_POST_PICK = (__ENV.HOT_POST_PICK || 'hash').toLowerCase();
const HOT_POST_SEED = __ENV.HOT_POST_SEED || 'comment-service-8020';
const HOT_POST_IDS = parseList(__ENV.HOT_POST_IDS || '');
const TAIL_POST_IDS = parseList(__ENV.TAIL_POST_IDS || '');
const POST_ID_POOLS = buildPostIDPools(POST_IDS, HOT_POST_IDS, TAIL_POST_IDS, HOT_POST_PERCENT, HOT_POST_PICK, HOT_POST_SEED);
const COMMENT_ID = __ENV.COMMENT_ID || '90001';
const OPERATOR_ID = __ENV.OPERATOR_ID || '1';
const STUDENT_START = Number(__ENV.STUDENT_START || '200000');
const PAGE_SIZE = Number(__ENV.PAGE_SIZE || '20');
const PAGE_NUM = Number(__ENV.PAGE_NUM || '1');
const KEYWORD = __ENV.KEYWORD || '雅思';
const KEYWORDS = parseList(__ENV.KEYWORDS || KEYWORD);
const KEYWORD_MODE = (__ENV.KEYWORD_MODE || 'round_robin').toLowerCase();
const SAME_USER = (__ENV.SAME_USER || '').toLowerCase() === 'true';
const REQUEST_ID_PREFIX = __ENV.REQUEST_ID_PREFIX || 'k6';
const THINK_TIME = Number(__ENV.THINK_TIME || '0');
const TRACE_SAMPLES_PER_VU = Number(__ENV.TRACE_SAMPLES_PER_VU || '1');
const ALLOW_BUSINESS_ERRORS = (__ENV.ALLOW_BUSINESS_ERRORS || '').toLowerCase() === 'true';
const CACHE_MODE = __ENV.CACHE_MODE || '';
const LOG_RESPONSE = (__ENV.LOG_RESPONSE || '').toLowerCase() === 'true';

export const options = {
  scenarios: buildScenarioOptions(),
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)'],
  thresholds: buildThresholds(),
};

function buildScenarioOptions() {
  const stages = parseStages(__ENV.STAGES || '');
  if (stages.length > 0) {
    return {
      load: {
        executor: 'ramping-vus',
        stages,
        gracefulRampDown: '10s',
      },
    };
  }

  const iterations = Number(__ENV.ITERATIONS || '0');
  if (Number.isFinite(iterations) && iterations > 0) {
    return {
      load: {
        executor: 'shared-iterations',
        vus: Number(__ENV.VUS || '1'),
        iterations,
        maxDuration: __ENV.DURATION || '30s',
      },
    };
  }

  return {
    load: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || '20'),
      duration: __ENV.DURATION || '30s',
      gracefulStop: '10s',
    },
  };
}

function parseStages(raw) {
  if (!raw) {
    return [];
  }
  return raw.split(',').map((item) => {
    const [duration, target] = item.split(':');
    return { duration, target: Number(target) };
  }).filter((stage) => stage.duration && Number.isFinite(stage.target));
}

function buildThresholds() {
  const thresholds = {
    checks: [`rate>${__ENV.MIN_CHECK_RATE || '0.95'}`],
    http_req_duration: [
      `p(95)<${__ENV.MAX_P95_MS || '500'}`,
      `p(99)<${__ENV.MAX_P99_MS || '1000'}`,
    ],
  };
  if (!ALLOW_BUSINESS_ERRORS) {
    thresholds.http_req_failed = [`rate<${__ENV.MAX_FAILED_RATE || '0.05'}`];
  }
  return thresholds;
}

export default function () {
  const seq = exec.scenario.iterationInTest;
  const postID = pickPostID(seq);
  const requestId = `${REQUEST_ID_PREFIX}-${SCENARIO}-vu${exec.vu.idInTest}-${seq}`;
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'x-request-id': requestId,
    },
    tags: {
      scenario_name: SCENARIO,
      post_id: postID,
    },
  };
  if (CACHE_MODE) {
    params.headers['x-cache-mode'] = CACHE_MODE;
  }

  const res = dispatch(params, seq, postID);
  const ok = check(res, {
    'status is expected': (r) => isExpectedStatus(r.status),
    'has trace id': (r) => Boolean(r.headers['X-Trace-Id'] || r.headers['x-trace-id']),
  });

  if (!ok) {
    console.error(`request failed request_id=${requestId} post_id=${postID} status=${res.status} body=${truncate(res.body, 300)}`);
  }

  const traceID = res.headers['X-Trace-Id'] || res.headers['x-trace-id'];
  if (traceID && exec.scenario.iterationInInstance < TRACE_SAMPLES_PER_VU) {
    console.log(`trace sample scenario=${SCENARIO} request_id=${requestId} trace_id=${traceID} status=${res.status} duration_ms=${res.timings.duration}`);
  }

  if (THINK_TIME > 0) {
    sleep(THINK_TIME);
  }
}

function isExpectedStatus(status) {
  if (status >= 200 && status < 300) {
    return true;
  }
  // 重复点赞、重复取消、重复审核这类并发冲突，合理结果应该是 4xx 业务错误。
  // 如果返回 5xx，说明错误码映射还不够规范，需要继续修。
  return ALLOW_BUSINESS_ERRORS && status >= 400 && status < 500;
}

function dispatch(params, seq, postID) {
  switch (SCENARIO) {
    case 'student-post-detail':
      return studentPostDetail(params, postID);
    case 'student-comment-list':
      return studentCommentList(params, postID);
    case 'student-post-search':
      return studentPostSearch(params, seq);
    case 'student-comment-search':
      return studentCommentSearch(params, postID, seq);
    case 'like-post':
      return likePost(params, seq, postID);
    case 'unlike-post':
      return unlikePost(params, seq, postID);
    case 'create-comment':
      return createComment(params, seq, postID);
    case 'audit-approve':
      return auditComment(params, 1);
    case 'audit-reject':
      return auditComment(params, 2);
    default:
      throw new Error(`unknown SCENARIO=${SCENARIO}`);
  }
}

function studentPostDetail(params, postID) {
  const url = `${BASE_URL}/v1/student/posts/${postID}?comment_page_num=${PAGE_NUM}&comment_page_size=${PAGE_SIZE}`;
  return http.get(url, params);
}

function studentCommentList(params, postID) {
  const url = `${BASE_URL}/v1/student/posts/${postID}/comments?page_num=${PAGE_NUM}&page_size=${PAGE_SIZE}`;
  return http.get(url, params);
}

function studentPostSearch(params, seq) {
  const keyword = pickKeyword(seq);
  const query = encodeQuery({
    keyword,
    page_num: PAGE_NUM,
    page_size: PAGE_SIZE,
  });
  return http.get(`${BASE_URL}/v1/student/search/posts?${query}`, params);
}

function studentCommentSearch(params, postID, seq) {
  const keyword = pickKeyword(seq);
  const query = encodeQuery({
    keyword,
    post_id: postID,
    page_num: PAGE_NUM,
    page_size: PAGE_SIZE,
  });
  return http.get(`${BASE_URL}/v1/student/search/comments?${query}`, params);
}

function likePost(params, seq, postID) {
  const body = JSON.stringify({
    student_id: String(studentID(seq)),
    post_id: postID,
  });
  return http.post(`${BASE_URL}/v1/student/posts/${postID}/like`, body, params);
}

function unlikePost(params, seq, postID) {
  const query = encodeQuery({ student_id: String(studentID(seq)) });
  return http.del(`${BASE_URL}/v1/student/posts/${postID}/like?${query}`, null, params);
}

function createComment(params, seq, postID) {
  const body = JSON.stringify({
    student_id: String(studentID(seq)),
    post_id: postID,
    content: `k6 load test comment ${seq} from vu ${exec.vu.idInTest}`,
  });
  const res = http.post(`${BASE_URL}/v1/student/posts/${postID}/comments`, body, params);
  if (LOG_RESPONSE && res.status >= 200 && res.status < 300) {
    console.log(`create comment result post_id=${postID} body=${truncate(res.body, 500)}`);
  }
  return res;
}

function auditComment(params, action) {
  const body = {
    operator_id: OPERATOR_ID,
    comment_id: COMMENT_ID,
    action,
  };
  if (action === 2) {
    body.manual_review_reason = 'k6 load test reject';
  }
  return http.post(`${BASE_URL}/v1/operator/comments/${COMMENT_ID}/audit`, JSON.stringify(body), params);
}

function studentID(seq) {
  if (SAME_USER) {
    return STUDENT_START;
  }
  return STUDENT_START + exec.vu.idInTest * 1000000 + seq;
}

function pickPostID(seq) {
  if (POST_IDS.length === 0) {
    return POST_ID;
  }
  if (isHotspotPostIDMode()) {
    return pickHotspotPostID();
  }
  if (POST_ID_MODE === 'random') {
    return randomFrom(POST_IDS);
  }
  return POST_IDS[seq % POST_IDS.length];
}

function pickKeyword(seq) {
  if (KEYWORDS.length === 0) {
    return KEYWORD;
  }
  if (KEYWORD_MODE === 'random') {
    return randomFrom(KEYWORDS);
  }
  return KEYWORDS[seq % KEYWORDS.length];
}

function isHotspotPostIDMode() {
  return ['hotspot', 'hotspot_80_20', 'weighted', 'weighted_80_20', 'pareto_80_20'].includes(POST_ID_MODE);
}

function pickHotspotPostID() {
  const useHotPool = Math.random() * 100 < HOT_TRAFFIC_PERCENT;
  const primaryPool = useHotPool ? POST_ID_POOLS.hot : POST_ID_POOLS.tail;
  const fallbackPool = useHotPool ? POST_ID_POOLS.tail : POST_ID_POOLS.hot;

  if (primaryPool.length > 0) {
    return randomFrom(primaryPool);
  }
  if (fallbackPool.length > 0) {
    return randomFrom(fallbackPool);
  }
  return randomFrom(POST_IDS);
}

function buildPostIDPools(postIDs, hotPostIDs, tailPostIDs, hotPostPercent, hotPostPick, hotPostSeed) {
  const basePostIDs = uniqueList(postIDs);
  const hotPool = hotPostIDs.length > 0
    ? uniqueList(hotPostIDs)
    : selectSyntheticHotPostIDs(basePostIDs, hotPostPercent, hotPostPick, hotPostSeed);

  if (tailPostIDs.length > 0) {
    return { hot: hotPool, tail: uniqueList(tailPostIDs) };
  }

  const hotSet = new Set(hotPool);
  return {
    hot: hotPool,
    tail: basePostIDs.filter((postID) => !hotSet.has(postID)),
  };
}

function selectSyntheticHotPostIDs(postIDs, hotPostPercent, hotPostPick, hotPostSeed) {
  const count = Math.max(1, Math.ceil(postIDs.length * hotPostPercent / 100));
  if (hotPostPick === 'first') {
    return postIDs.slice(0, count);
  }
  return stableShuffle(postIDs, hotPostSeed).slice(0, count);
}

function stableShuffle(values, seed) {
  return [...values].sort((left, right) => hashString(`${seed}:${left}`) - hashString(`${seed}:${right}`));
}

function hashString(value) {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function randomFrom(values) {
  return values[Math.floor(Math.random() * values.length)];
}

function uniqueList(values) {
  const seen = new Set();
  const result = [];
  for (const value of values) {
    if (seen.has(value)) {
      continue;
    }
    seen.add(value);
    result.push(value);
  }
  return result;
}

function clampPercent(value, fallback) {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(0, Math.min(100, value));
}

function parseList(raw) {
  return raw.split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function encodeQuery(values) {
  return Object.entries(values)
    .filter(([, value]) => value !== undefined && value !== null && value !== '')
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`)
    .join('&');
}

function truncate(value, max) {
  if (!value || value.length <= max) {
    return value || '';
  }
  return `${value.slice(0, max)}...`;
}

export function handleSummary(data) {
  const output = {
    stdout: textSummary(data),
  };
  const path = __ENV.SUMMARY_PATH;
  if (path) {
    output[path] = JSON.stringify(data, null, 2);
  }
  return output;
}

function textSummary(data) {
  const metrics = data.metrics;
  const duration = metrics.http_req_duration || {};
  const failed = metrics.http_req_failed || {};
  const reqs = metrics.http_reqs || {};
  const checks = metrics.checks || {};

  return [
    '',
    'comment-service k6 summary',
    '--------------------------',
    `scenario:      ${SCENARIO}`,
    `base_url:      ${BASE_URL}`,
    `post_ids:      ${POST_IDS.length}`,
    `post_id_mode:  ${POST_ID_MODE}`,
    ...postIDModeSummaryLines(),
    ...keywordSummaryLines(),
    `cache_mode:    ${CACHE_MODE || 'default'}`,
    `requests:      ${metricCount(reqs)}`,
    `http_failed:   ${metricRate(failed)}`,
    `checks_rate:   ${metricRate(checks)}`,
    `latency_avg:   ${metricValue(duration, 'avg')} ms`,
    `latency_p50:   ${metricValue(duration, 'med')} ms`,
    `latency_p90:   ${metricValue(duration, 'p(90)')} ms`,
    `latency_p95:   ${metricValue(duration, 'p(95)')} ms`,
    `latency_p99:   ${metricValue(duration, 'p(99)')} ms`,
    '',
  ].join('\n');
}

function postIDModeSummaryLines() {
  if (!isHotspotPostIDMode()) {
    return [];
  }
  return [
    `hot_posts:     ${POST_ID_POOLS.hot.length}`,
    `tail_posts:    ${POST_ID_POOLS.tail.length}`,
    `hot_traffic:   ${HOT_TRAFFIC_PERCENT.toFixed(0)}%`,
    `hot_pick:      ${HOT_POST_IDS.length > 0 ? 'manual' : HOT_POST_PICK}`,
  ];
}

function keywordSummaryLines() {
  if (!['student-post-search', 'student-comment-search'].includes(SCENARIO)) {
    return [];
  }
  return [
    `keywords:      ${KEYWORDS.length}`,
    `keyword_mode:  ${KEYWORD_MODE}`,
  ];
}

function metricValue(metric, key) {
  const value = metric.values && metric.values[key];
  return typeof value === 'number' ? value.toFixed(2) : '0.00';
}

function metricRate(metric) {
  const value = metric.values && metric.values.rate;
  return typeof value === 'number' ? `${(value * 100).toFixed(2)}%` : '0.00%';
}

function metricCount(metric) {
  const value = metric.values && metric.values.count;
  return typeof value === 'number' ? String(value) : '0';
}
