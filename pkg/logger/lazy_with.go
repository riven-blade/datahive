package logger

import (
	"sync"
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

// lazyWithCore wraps zapcore.Core with lazy initialization.
// Copied from https://github.com/uber-go/zap/issues/1426 to avoid data race.
type lazyWithCore struct {
	corePtr atomic.Pointer[zapcore.Core]
	once    sync.Once
	fields  []zapcore.Field
}

var _ zapcore.Core = (*lazyWithCore)(nil)

func NewLazyWith(core zapcore.Core, fields []zapcore.Field) zapcore.Core {
	d := lazyWithCore{fields: fields}
	d.corePtr.Store(&core)
	return &d
}

func (d *lazyWithCore) initOnce() zapcore.Core {
	core := *d.corePtr.Load()
	d.once.Do(func() {
		core = core.With(d.fields)
		d.corePtr.Store(&core)
	})
	return core
}

func (d *lazyWithCore) Enabled(level zapcore.Level) bool {
	// Init not needed
	return (*d.corePtr.Load()).Enabled(level)
}

func (d *lazyWithCore) Sync() error {
	// Init needed
	return d.initOnce().Sync()
}

// Write implements zapcore.Core.
func (d *lazyWithCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return (*d.corePtr.Load()).Write(entry, fields)
}

func (d *lazyWithCore) With(fields []zapcore.Field) zapcore.Core {
	d.initOnce()
	return (*d.corePtr.Load()).With(fields)
}

func (d *lazyWithCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	d.initOnce()
	return (*d.corePtr.Load()).Check(e, ce)
}
