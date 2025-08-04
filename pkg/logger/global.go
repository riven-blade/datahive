package logger

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ctxLogKeyType struct{}

var CtxLogKey = ctxLogKeyType{}

// SetLevel alters the logging level.
func SetLevel(l zapcore.Level) {
	_globalP.Load().(*ZapProperties).Level.SetLevel(l)
}

// GetLevel gets the logging level.
func GetLevel() zapcore.Level {
	return _globalP.Load().(*ZapProperties).Level.Level()
}

// WithTraceID returns a context with trace_id attached
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return WithFields(ctx, zap.String("traceID", traceID))
}

// WithReqID adds given reqID field to the logger in ctx
func WithReqID(ctx context.Context, reqID int64) context.Context {
	fields := []zap.Field{zap.Int64("reqID", reqID)}
	return WithFields(ctx, fields...)
}

// WithModule adds given module field to the logger in ctx
func WithModule(ctx context.Context, module string) context.Context {
	fields := []zap.Field{zap.String(FieldNameModule, module)}
	return WithFields(ctx, fields...)
}

// WithFields returns a context with fields attached
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	var zlogger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*MLogger); ok {
		zlogger = ctxLogger.Logger
	} else {
		zlogger = ctxL()
	}
	mLogger := &MLogger{
		Logger: zlogger.With(fields...),
	}
	return context.WithValue(ctx, CtxLogKey, mLogger)
}

// NewIntentContext creates a new context with intent information and returns it along with a span.
func NewIntentContext(name string, intent string) (context.Context, trace.Span) {
	intentCtx, initSpan := otel.Tracer(name).Start(context.Background(), intent)
	intentCtx = WithFields(intentCtx,
		zap.String("role", name),
		zap.String("intent", intent),
		zap.String("traceID", initSpan.SpanContext().TraceID().String()))
	return intentCtx, initSpan
}

// Ctx returns a logger which will log contextual messages attached in ctx
func Ctx(ctx context.Context) *MLogger {
	if ctx == nil {
		return &MLogger{Logger: ctxL()}
	}
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*MLogger); ok {
		return ctxLogger
	}
	return &MLogger{Logger: ctxL()}
}

// withLogLevel returns ctx with a leveled logger, notes that it will overwrite logger previous attached!
func withLogLevel(ctx context.Context, level zapcore.Level) context.Context {
	var zlogger *zap.Logger
	switch level {
	case zap.DebugLevel:
		zlogger = debugL()
	case zap.InfoLevel:
		zlogger = infoL()
	case zap.WarnLevel:
		zlogger = warnL()
	case zap.ErrorLevel:
		zlogger = errorL()
	case zap.FatalLevel:
		zlogger = fatalL()
	default:
		zlogger = L()
	}
	return context.WithValue(ctx, CtxLogKey, &MLogger{Logger: zlogger})
}

// WithDebugLevel returns context with a debug level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithDebugLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.DebugLevel)
}

// WithInfoLevel returns context with a info level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithInfoLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.InfoLevel)
}

// WithWarnLevel returns context with a warning level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithWarnLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.WarnLevel)
}

// WithErrorLevel returns context with a error level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithErrorLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.ErrorLevel)
}

// WithFatalLevel returns context with a fatal level enabled logger.
// Notes that it will overwrite previous attached logger within context
func WithFatalLevel(ctx context.Context) context.Context {
	return withLogLevel(ctx, zapcore.FatalLevel)
}

// ========== 全局日志函数 ==========

func Debug(msg string, fields ...zap.Field) {
	debugL().Debug(msg, fields...)
}

// Info logs a message at InfoLevel.
func Info(msg string, fields ...zap.Field) {
	infoL().Info(msg, fields...)
}

// Warn logs a message at WarnLevel.
func Warn(msg string, fields ...zap.Field) {
	warnL().Warn(msg, fields...)
}

// Error logs a message at ErrorLevel.
func Error(msg string, fields ...zap.Field) {
	errorL().Error(msg, fields...)
}

// Panic logs a message at PanicLevel and panics.
func Panic(msg string, fields ...zap.Field) {
	L().Panic(msg, fields...)
}

// With creates a child logger and adds structured context to it.
func With(fields ...zap.Field) *MLogger {
	return &MLogger{Logger: L().With(fields...)}
}

// RatedDebug logs a message at DebugLevel with rate limiting.
func RatedDebug(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		debugL().Debug(msg, fields...)
		return true
	}
	return false
}

// RatedInfo logs a message at InfoLevel with rate limiting.
func RatedInfo(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		infoL().Info(msg, fields...)
		return true
	}
	return false
}

// RatedWarn logs a message at WarnLevel with rate limiting.
func RatedWarn(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		warnL().Warn(msg, fields...)
		return true
	}
	return false
}
