package logger

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
)

type foo struct {
	Key   string
	Value string
}

func BenchmarkZapReflect(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		With(zap.Any("payload", payload))
	}
}

func BenchmarkZapWithLazy(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		L().WithLazy(zap.Any("payload", payload))
	}
}

// The following two benchmarks are validations if `WithLazy` has the same performance as `With` in the worst case.
func BenchmarkWithLazyLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := L().WithLazy(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}

func BenchmarkWithLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := L().With(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}
