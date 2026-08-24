package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var manualTracer = otel.Tracer("comment-service/manual")

// StartSpan 创建业务手动埋点 span。
//
// Kratos tracing.Server() 只会自动创建入口 span；service/biz/data/Redis/MySQL
// 这些内部步骤需要显式调用 StartSpan，Jaeger 才能看到更细的瀑布图。
//
// 返回值：
// 1. context.Context：携带新 span 的上下文，后续下游调用要继续传它；
// 2. trace.Span：当前埋点 span，函数退出前必须调用 EndSpan。
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// manualTracer 使用固定 instrumentation name，Jaeger 中可看到 comment-service/manual scope。
	ctx, span := manualTracer.Start(ctx, name)
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

// EndSpan 结束 span，并在 err 非空时记录错误状态。
//
// 调用方通常写成 defer func(){ EndSpan(span, err) }，
// 这样函数返回错误时 Jaeger 中的 span 会自动标红并带上错误信息。
func EndSpan(span trace.Span, err error) {
	if err != nil {
		// RecordError 写 exception event，SetStatus 写 otel.status_code/status_description。
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
