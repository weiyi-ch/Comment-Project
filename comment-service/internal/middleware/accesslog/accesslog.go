package accesslog

import (
	"context"
	"time"

	kerrors "github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/http/status"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
)

// 访问日志和响应头使用的追踪字段名。
//
// x-request-id 由客户端或压测工具传入，x-trace-id 由 OpenTelemetry span 派生后回写。
const (
	headerRequestID = "x-request-id"
	headerTraceID   = "x-trace-id"
)

// Server 为每个进入服务的 HTTP/gRPC 请求记录一条结构化访问日志。
//
// 日志字段保持简洁稳定：
// operation + request_id + trace_id + latency + code 足够在压测和面试讲解时串起一次请求，
// 同时避免记录请求体和敏感字段。
func Server(logger log.Logger) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (reply any, err error) {
			// start 用于计算接口总耗时，覆盖 middleware 后续的 service/biz/data 调用。
			start := time.Now()

			var (
				kind      string
				operation string
				requestID string
			)
			if tr, ok := transport.FromServerContext(ctx); ok {
				// transport 保存协议类型、RPC 路由名和请求头。
				kind = tr.Kind().String()
				operation = tr.Operation()
				requestID = tr.RequestHeader().Get(headerRequestID)
			}

			// handler 是下一个中间件或最终 service 方法，返回业务响应和错误。
			reply, err = handler(ctx, req)

			// trace.SpanContextFromContext 返回当前请求入口 span 的 trace_id/span_id。
			spanCtx := trace.SpanContextFromContext(ctx)
			traceID := ""
			spanID := ""
			if spanCtx.HasTraceID() {
				traceID = spanCtx.TraceID().String()
			}
			if spanCtx.HasSpanID() {
				spanID = spanCtx.SpanID().String()
			}
			if requestID == "" {
				requestID = traceID
			}

			if tr, ok := transport.FromServerContext(ctx); ok {
				// 把 trace_id/request_id 写回响应头，Postman/k6 可以直接看到本次请求对应的链路。
				if traceID != "" {
					tr.ReplyHeader().Set(headerTraceID, traceID)
				}
				if requestID != "" {
					tr.ReplyHeader().Set(headerRequestID, requestID)
				}
			}

			code := int32(status.FromGRPCCode(codes.OK))
			reason := ""
			if err != nil {
				code = int32(status.FromGRPCCode(codes.Unknown))
				reason = err.Error()
				// Kratos errors 会携带 HTTP code 和 reason，优先使用它们，让日志和响应一致。
				if se := kerrors.FromError(err); se != nil {
					code = se.Code
					reason = se.Reason
				}
			}

			level := log.LevelInfo
			if err != nil {
				level = log.LevelError
			}

			log.NewHelper(log.WithContext(ctx, logger)).Log(level,
				"event", "access",
				"kind", kind,
				"operation", operation,
				"request_id", requestID,
				"trace_id", traceID,
				"span_id", spanID,
				"code", code,
				"reason", reason,
				"latency_ms", time.Since(start).Milliseconds(),
			)

			return reply, err
		}
	}
}
