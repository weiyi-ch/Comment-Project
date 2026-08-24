package observability

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// tracing 初始化默认值。
//
// 默认全量采样更适合本地演示、压测复盘和面试讲解链路。
const (
	defaultTraceInitTimeout = 5 * time.Second
	defaultTraceSampleRatio = 1.0
)

// TraceConfig 描述链路追踪导出配置。
//
// 当前不放进 proto 配置，是为了不增加配置生成成本；本地演示和压测用环境变量控制即可。
type TraceConfig struct {
	ServiceName    string
	ServiceVersion string
	Endpoint       string
	Enabled        bool
	SampleRatio    float64
}

// InitTracer 初始化全局 OpenTelemetry TracerProvider。
//
// Kratos 的 tracing.Server() 会从全局 TracerProvider 创建 server span。
// 如果不初始化 exporter，trace_id 只能在日志里看到，Jaeger/Tempo 这类 UI 收不到 span。
func InitTracer(ctx context.Context, cfg TraceConfig) (func(context.Context) error, error) {
	// fillTraceConfigFromEnv 返回合并环境变量后的配置，方便本地通过 TRACE_ENABLED/OTEL_* 开启追踪。
	cfg = fillTraceConfigFromEnv(cfg)
	if !cfg.Enabled {
		// 即使不导出到 Jaeger，也要设置 propagator，保证 traceparent 透传逻辑一致。
		otel.SetTextMapPropagator(tracePropagator())
		return nil, nil
	}

	// 初始化 exporter 设置超时，避免 Jaeger/Collector 不可用时服务启动长时间卡住。
	initCtx, cancel := context.WithTimeout(ctx, defaultTraceInitTimeout)
	defer cancel()

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithInsecure(),
	}
	if cfg.Endpoint != "" {
		// EndpointURL 支持 http://localhost:4317 这种完整 URL；Endpoint 支持 localhost:4317。
		if strings.HasPrefix(cfg.Endpoint, "http://") || strings.HasPrefix(cfg.Endpoint, "https://") {
			opts = append(opts, otlptracegrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
	}

	// exporter 负责把 span 通过 OTLP/gRPC 发给 Collector 或 Jaeger。
	exporter, err := otlptracegrpc.New(initCtx, opts...)
	if err != nil {
		return nil, err
	}

	// resource 是每条 trace 附带的服务元信息，Jaeger UI 会按 service.name 分组。
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"",
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion),
			attribute.String("deployment.environment", getenv("APP_ENV", "local")),
		),
	)
	if err != nil {
		return nil, err
	}

	// TracerProvider 统一配置 batcher、resource、采样率；ParentBased 保证上游已采样时下游继续采样。
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(tracePropagator())

	return tp.Shutdown, nil
}

// fillTraceConfigFromEnv 合并代码传入配置和环境变量配置。
//
// 返回值 TraceConfig 会保证 ServiceName、ServiceVersion、SampleRatio 都有有效默认值。
func fillTraceConfigFromEnv(cfg TraceConfig) TraceConfig {
	if cfg.ServiceName == "" {
		cfg.ServiceName = getenv("OTEL_SERVICE_NAME", "comment-service")
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = getenv("SERVICE_VERSION", "v1")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	cfg.Enabled = cfg.Enabled || parseBool(os.Getenv("TRACE_ENABLED")) || cfg.Endpoint != ""
	cfg.SampleRatio = normalizeSampleRatio(cfg.SampleRatio)
	if v := os.Getenv("TRACE_SAMPLE_RATIO"); v != "" {
		if ratio, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.SampleRatio = normalizeSampleRatio(ratio)
		}
	}

	return cfg
}

// tracePropagator 返回 trace 上下文传播器。
//
// HTTP/gRPC 请求头中的 traceparent/baggage 会由它解析和注入。
func tracePropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// normalizeSampleRatio 规范化采样率。
//
// 返回值范围是 (0,1]；传 0 或负数时使用默认全量采样，便于本地排障。
func normalizeSampleRatio(v float64) float64 {
	if v <= 0 {
		return defaultTraceSampleRatio
	}
	if v > 1 {
		return 1
	}
	return v
}

// parseBool 把环境变量字符串解析为布尔值。
//
// 返回 true 的值用于 TRACE_ENABLED 这类开关。
func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

// getenv 读取环境变量，未设置时返回 fallback。
//
// 该 helper 让 tracing 初始化逻辑保持可读。
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
