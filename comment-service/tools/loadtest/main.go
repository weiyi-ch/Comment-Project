package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// config 保存一次压测运行所需的所有参数。
type config struct {
	baseURL         string        // 目标服务的基准 URL (例如 http://127.0.0.1:8888)
	scenario        string        // 压测场景 (具体支持的业务 API 场景)
	duration        time.Duration // 压测持续总时间 (例如 30s)
	concurrency     int           // 并发 Worker 数量 (协程数)
	postID          int64         // 帖子 ID (读写帖子相关场景使用)
	commentID       int64         // 评论 ID (审核相关场景使用)
	operatorID      int64         // 审核操作员 ID
	studentStart    int64         // 自增学生 ID 的起始值，用于模拟不同用户
	pageSize        int           // 分页查询的大小
	keyword         string        // 搜索场景的关键字
	sameUser        bool          // 是否复用同一个学生 ID (用于测试并发锁或幂等性)
	timeout         time.Duration // 单次 HTTP 请求的超时时间
	requestIDPrefix string        // x-request-id 的前缀，便于在后端日志中捞出压测流量
	traceSamples    int           // 最终报告中打印的 trace_id 样本数量
}

// result 表示单个 HTTP 请求的压测结果。
type result struct {
	status    int           // HTTP 状态码 (如 200, 500)
	latency   time.Duration // 耗时/延迟
	err       string        // 错误信息 (如果请求失败)
	requestID string        // 发出请求时携带的 request_id
	traceID   string        // 后端返回的 trace_id
}

// main 启动本地压测工具。
// 工具会按配置创建固定数量 worker，在指定 duration 内持续发请求，最后汇总 QPS、状态码、错误率和延迟分位数。
func main() {
	// 1. 解析并校验命令行输入参数
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		os.Exit(2)
	}

	// 2. 初始化高并发安全的 HTTP 客户端与生命周期上下文
	// 多个 Goroutine 共享同一个 client 是安全的，底层会自动复用连接池（Keep-Alive）
	client := &http.Client{Timeout: cfg.timeout}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)
	defer cancel()

	// 3. 创建带有缓冲区的通道，用于收集所有协程产生的请求结果
	// 容量设为并发数的 1024 倍，防止因主协程消费慢而导致 Worker 协程阻塞
	results := make(chan result, cfg.concurrency*1024)
	var sent atomic.Int64 // 原子计数器，多协程安全地自增生成请求序列号
	var wg sync.WaitGroup

	start := time.Now()
	// 4. 衍生指定数量的并发 Worker 协程
	for workerID := 0; workerID < cfg.concurrency; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				// 检查压测时间是否结束
				select {
				case <-ctx.Done():
					return
				default:
				}

				// 发出请求数原子加 1，作为当前请求的唯一序号
				seq := sent.Add(1)
				// 核心：基于当前场景构造 HTTP Request 对象
				req, err := buildRequest(cfg, workerID, seq)
				if err != nil {
					results <- result{err: err.Error()}
					continue
				}

				// 构造并注入 x-request-id 头部，方便与后端日志和微服务 Trace 锚定
				requestID := buildRequestID(cfg, workerID, seq)
				if requestID != "" {
					req.Header.Set("x-request-id", requestID)
				}

				// 将带有超时控制的 ctx 注入请求中
				req = req.WithContext(ctx)

				t0 := time.Now()
				// 执行真实的 HTTP 网络请求
				resp, err := client.Do(req)
				latency := time.Since(t0) // 计算单次请求耗时

				if err != nil {
					// 网络连接失败、DNS 解析错误或网关超时等情况
					results <- result{latency: latency, err: err.Error(), requestID: requestID}
					continue
				}

				// 提取后端微服务回传的 Trace ID
				traceID := resp.Header.Get("x-trace-id")

				// 重要：必须把 Body 读完并关闭，否则无法复用 TCP 长连接，会导致大量的 TIME_WAIT
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				// 将成功捕获的结果投递进 channel
				results <- result{status: resp.StatusCode, latency: latency, requestID: requestID, traceID: traceID}
			}
		}(workerID)
	}

	// 5. 等待所有 Worker 运行完毕（即 ctx 超时退出）
	wg.Wait()
	close(results) // 关闭通道，通知报告函数停止读取

	// 6. 汇总计算并生成美观的压测报告
	report(cfg, time.Since(start), results)
}

// parseFlags 解析命令行参数，并对 baseURL 做基础规范化。
func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.baseURL, "base-url", "http://127.0.0.1:8888", "service base URL")
	flag.StringVar(&cfg.scenario, "scenario", "student-post-detail", "student-post-detail|student-comment-list|student-post-search|like-post|unlike-post|create-comment|audit-approve|audit-reject")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "test duration")
	flag.IntVar(&cfg.concurrency, "concurrency", 20, "number of concurrent workers")
	flag.Int64Var(&cfg.postID, "post-id", 0, "post id")
	flag.Int64Var(&cfg.commentID, "comment-id", 0, "comment id")
	flag.Int64Var(&cfg.operatorID, "operator-id", 1, "operator id")
	flag.Int64Var(&cfg.studentStart, "student-start", 100000, "first student id used by write scenarios")
	flag.IntVar(&cfg.pageSize, "page-size", 20, "page size")
	flag.StringVar(&cfg.keyword, "keyword", "雅思", "search keyword")
	flag.BoolVar(&cfg.sameUser, "same-user", false, "reuse one student id to test idempotency")
	flag.DurationVar(&cfg.timeout, "timeout", 3*time.Second, "per request timeout")
	flag.StringVar(&cfg.requestIDPrefix, "request-id-prefix", "loadtest", "x-request-id prefix; empty disables request id header")
	flag.IntVar(&cfg.traceSamples, "trace-samples", 10, "number of x-trace-id samples to print")
	flag.Parse()
	cfg.baseURL = strings.TrimRight(cfg.baseURL, "/") // 去除末尾的斜杠，防止拼接 URL 时出现双斜杠 //
	return cfg
}

// validateConfig 校验场景所需参数是否齐全，避免压测时发出无效请求。
func validateConfig(cfg config) error {
	if cfg.concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive")
	}
	if cfg.duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	// 防御式编程：检查特定场景是否遗漏了必要的参数，防止盲目压测产生大量的 400 坏请求
	switch cfg.scenario {
	case "student-post-detail", "student-comment-list", "like-post", "unlike-post", "create-comment":
		if cfg.postID <= 0 {
			return fmt.Errorf("%s requires -post-id", cfg.scenario)
		}
	case "audit-approve", "audit-reject":
		if cfg.commentID <= 0 {
			return fmt.Errorf("%s requires -comment-id", cfg.scenario)
		}
	case "student-post-search":
	default:
		return fmt.Errorf("unknown scenario %q", cfg.scenario)
	}
	return nil
}

// buildRequest 根据压测场景构造 HTTP 请求。
// 每个场景对应 comment-service 的一个核心接口，便于分别压测读链路、写链路和审核状态机。
func buildRequest(cfg config, workerID int, seq int64) (*http.Request, error) {
	switch cfg.scenario {
	// 场景 1：学生查看帖子详情
	case "student-post-detail":
		u := fmt.Sprintf("%s/v1/student/posts/%d?comment_page_num=1&comment_page_size=%d", cfg.baseURL, cfg.postID, cfg.pageSize)
		return http.NewRequest(http.MethodGet, u, nil)

	// 场景 2：学生查看评论列表
	case "student-comment-list":
		u := fmt.Sprintf("%s/v1/student/posts/%d/comments?page_num=1&page_size=%d", cfg.baseURL, cfg.postID, cfg.pageSize)
		return http.NewRequest(http.MethodGet, u, nil)

	// 场景 3：帖子检索（带 Query 参数编码）
	case "student-post-search":
		q := url.Values{}
		q.Set("keyword", cfg.keyword)
		q.Set("page_num", "1")
		q.Set("page_size", fmt.Sprintf("%d", cfg.pageSize))
		u := fmt.Sprintf("%s/v1/student/search/posts?%s", cfg.baseURL, q.Encode())
		return http.NewRequest(http.MethodGet, u, nil)

	// 场景 4：给帖子点赞（POST JSON）
	case "like-post":
		body := map[string]int64{
			"student_id": studentID(cfg, workerID, seq),
			"post_id":    cfg.postID,
		}
		return jsonRequest(http.MethodPost, fmt.Sprintf("%s/v1/student/posts/%d/like", cfg.baseURL, cfg.postID), body)

	// 场景 5：取消点赞（DELETE）
	case "unlike-post":
		u := fmt.Sprintf("%s/v1/student/posts/%d/like?student_id=%d", cfg.baseURL, cfg.postID, studentID(cfg, workerID, seq))
		return http.NewRequest(http.MethodDelete, u, nil)

	// 场景 6：创建评论
	case "create-comment":
		body := map[string]any{
			"student_id": studentID(cfg, workerID, seq),
			"post_id":    cfg.postID,
			"content":    fmt.Sprintf("load-test comment %d from worker %d", seq, workerID),
		}
		return jsonRequest(http.MethodPost, fmt.Sprintf("%s/v1/student/posts/%d/comments", cfg.baseURL, cfg.postID), body)

	// 场景 7：运营审核通过
	case "audit-approve":
		body := map[string]any{
			"operator_id": cfg.operatorID,
			"comment_id":  cfg.commentID,
			"action":      1, // 状态机状态：1 代表通过
		}
		return jsonRequest(http.MethodPost, fmt.Sprintf("%s/v1/operator/comments/%d/audit", cfg.baseURL, cfg.commentID), body)

	// 场景 8：运营审核拒绝
	case "audit-reject":
		body := map[string]any{
			"operator_id":          cfg.operatorID,
			"comment_id":           cfg.commentID,
			"action":               2, // 状态机状态：2 代表拒绝
			"manual_review_reason": "load test reject",
		}
		return jsonRequest(http.MethodPost, fmt.Sprintf("%s/v1/operator/comments/%d/audit", cfg.baseURL, cfg.commentID), body)

	default:
		return nil, fmt.Errorf("unknown scenario %q", cfg.scenario)
	}
}

// studentID 为写场景生成学生 ID。
// same-user=true 时所有 worker 复用同一个 student_id，用于验证点赞幂等和并发保护。
func studentID(cfg config, workerID int, seq int64) int64 {
	if cfg.sameUser {
		return cfg.studentStart
	}
	// 分段生成 ID 算法：确保每个 Worker 之间生成的 ID 完全独立不冲突
	return cfg.studentStart + int64(workerID)*1_000_000 + seq
}

// buildRequestID 为每个压测请求生成稳定 request id。
// accesslog 会把 request_id 和 trace_id 一起打印出来，压测报告也会采样 trace_id，这样可以从压测结果跳到日志或 Jaeger UI 中查看单次慢请求链路。
func buildRequestID(cfg config, workerID int, seq int64) string {
	if cfg.requestIDPrefix == "" {
		return ""
	}
	// 生成有迹可循的 RequestID：前缀-场景-Worker号-请求序号
	return fmt.Sprintf("%s-%s-w%d-%d", cfg.requestIDPrefix, cfg.scenario, workerID, seq)
}

// jsonRequest 创建带 JSON body 和 Content-Type 的 HTTP 请求。
func jsonRequest(method, rawURL string, body any) (*http.Request, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// report 汇总压测结果并打印人类可读的报告。
func report(cfg config, elapsed time.Duration, results <-chan result) {
	var latencies []float64                             // 存放所有成功请求的延迟（用于计算分位数）
	statuses := map[int]int{}                           // 统计不同状态码的频次 (如 200: 5000次, 502: 10次)
	errors := map[string]int{}                          // 统计不同错误类型的频次
	var total int                                       // 总请求数
	traceSamples := make([]result, 0, cfg.traceSamples) // 链路追踪采样切片

	// 从 channel 中循环读取结果，直到 channel 被关闭
	for r := range results {
		total++
		if r.err != "" {
			errors[shortErr(r.err)]++
			continue
		}
		statuses[r.status]++
		// 将 time.Duration 转换为毫秒(ms)的 float64 格式
		latencies = append(latencies, float64(r.latency.Microseconds())/1000)

		// 如果返回了 TraceID，且采样数还没满，则收集起来供最终打印
		if r.traceID != "" && len(traceSamples) < cfg.traceSamples {
			traceSamples = append(traceSamples, r)
		}
	}

	// 对所有延迟进行升序排序，这是计算 P50, P95, P99 分位数的关键前提
	sort.Float64s(latencies)

	// 计算 2xx 成功请求的总数
	success := 0
	for code, count := range statuses {
		if code >= 200 && code < 300 {
			success += count
		}
	}

	// 打印性能压测标准看板
	fmt.Println("comment-service load test")
	fmt.Println("-------------------------")
	fmt.Printf("scenario:      %s\n", cfg.scenario)
	fmt.Printf("base_url:      %s\n", cfg.baseURL)
	fmt.Printf("duration:      %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("concurrency:   %d\n", cfg.concurrency)
	fmt.Printf("requests:      %d\n", total)
	fmt.Printf("success_2xx:   %d\n", success)
	fmt.Printf("qps:           %.2f\n", float64(total)/elapsed.Seconds()) // 每秒请求数 (吞吐量)
	fmt.Printf("error_rate:    %.2f%%\n", percent(total-success, total))
	fmt.Printf("latency_avg:   %.2f ms\n", avg(latencies))
	fmt.Printf("latency_p50:   %.2f ms\n", percentile(latencies, 50)) // 50% 的请求在此耗时以内
	fmt.Printf("latency_p95:   %.2f ms\n", percentile(latencies, 95)) // 95% 的请求在此耗时以内（衡量尾部延迟）
	fmt.Printf("latency_p99:   %.2f ms\n", percentile(latencies, 99)) // 99% 的请求在此耗时以内（极具参考价值的恶劣性能指标）

	fmt.Println()
	fmt.Println("status codes:")
	printIntMap(statuses)

	if len(errors) > 0 {
		fmt.Println()
		fmt.Println("errors:")
		printStringMap(errors)
	}

	// 打印 Trace 采样结果，这部分数据可以直接和分布式链路追踪系统打通
	if len(traceSamples) > 0 {
		fmt.Println()
		fmt.Println("trace samples:")
		for _, sample := range traceSamples {
			fmt.Printf("  request_id=%s trace_id=%s status=%d latency=%s\n",
				sample.requestID,
				sample.traceID,
				sample.status,
				sample.latency.Round(time.Millisecond),
			)
		}
	}
}

// avg 计算平均延迟。
func avg(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// percentile 计算延迟分位数，values 需要提前升序排列。
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	// 计算公式：容量 * 百分比，向上取整得到对应的索引排名
	rank := int(math.Ceil((p / 100) * float64(len(values))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(values) {
		rank = len(values)
	}
	return values[rank-1]
}

// percent 计算百分比，total 为 0 时返回 0 避免除零。
func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) * 100 / float64(total)
}

// printIntMap 按 key 升序打印 int map，主要用于状态码统计。
func printIntMap(values map[int]int) {
	keys := make([]int, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Printf("  %d: %d\n", k, values[k])
	}
}

// printStringMap 按 key 升序打印 string map，主要用于错误信息统计。
func printStringMap(values map[string]int) {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %s: %d\n", k, values[k])
	}
}

// shortErr 截断过长错误，避免压测报告被网络错误栈刷屏。
func shortErr(s string) string {
	if len(s) <= 120 {
		return s
	}
	return s[:120] + "..."
}
