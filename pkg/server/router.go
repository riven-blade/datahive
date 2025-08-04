package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datahive/pkg/logger"
	"datahive/pkg/protocol"

	"go.uber.org/zap"
)

// ErrNoHandler 没有找到处理器的错误
var ErrNoHandler = fmt.Errorf("no handler found")

// RouterStats 路由统计信息（不包含mutex）
type RouterStats struct {
	HandlersRegistered int              `json:"handlers_registered"`
	MessagesProcessed  int64            `json:"messages_processed"`
	MessagesSucceeded  int64            `json:"messages_succeeded"`
	MessagesFailed     int64            `json:"messages_failed"`
	AverageLatency     time.Duration    `json:"average_latency"`
	HandlerStats       map[string]int64 `json:"handler_stats"`
	LastMessageTime    time.Time        `json:"last_message_time"`
}

// Router 消息路由器接口
type Router interface {
	RegisterHandler(action protocol.ActionType, handler MessageHandler)
	UnregisterHandler(action protocol.ActionType)
	RouteMessage(conn Connection, msg *protocol.Message) error
	GetStats() *RouterStats
	GetHandlerCount() int
}

// StandardRouter 标准消息路由器
type StandardRouter struct {
	// 消息处理器映射
	handlers map[protocol.ActionType]MessageHandler
	mu       sync.RWMutex

	// 统计信息和它的互斥锁
	stats   *RouterStats
	statsMu sync.RWMutex
}

// NewRouter 创建消息路由器
func NewRouter() Router {
	return &StandardRouter{
		handlers: make(map[protocol.ActionType]MessageHandler),
		stats: &RouterStats{
			HandlerStats: make(map[string]int64),
		},
	}
}

// RegisterHandler 注册消息处理器
func (r *StandardRouter) RegisterHandler(action protocol.ActionType, handler MessageHandler) {
	ctx := context.Background()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlers[action] = handler

	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.stats.HandlersRegistered = len(r.handlers)

	logger.Ctx(ctx).Debug("Handler registered",
		zap.String("action", action.String()),
		zap.Int("total_handlers", len(r.handlers)))
}

// UnregisterHandler 注销消息处理器
func (r *StandardRouter) UnregisterHandler(action protocol.ActionType) {
	ctx := context.Background()

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.handlers, action)

	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.stats.HandlersRegistered = len(r.handlers)

	logger.Ctx(ctx).Debug("Handler unregistered",
		zap.String("action", action.String()),
		zap.Int("total_handlers", len(r.handlers)))
}

// RouteMessage 路由消息到对应的处理器
func (r *StandardRouter) RouteMessage(conn Connection, msg *protocol.Message) error {
	ctx := context.Background()
	startTime := time.Now()

	// 更新统计
	r.statsMu.Lock()
	r.stats.MessagesProcessed++
	r.stats.LastMessageTime = startTime
	r.statsMu.Unlock()

	// 查找处理器
	r.mu.RLock()
	handler, exists := r.handlers[msg.Action]
	r.mu.RUnlock()

	if !exists {
		logger.Ctx(ctx).Debug("No handler registered for action",
			zap.String("action", msg.Action.String()),
			zap.String("remote", conn.GetRemote()))

		r.statsMu.Lock()
		r.stats.MessagesFailed++
		r.statsMu.Unlock()

		return ErrNoHandler
	}

	// 调用处理器
	err := handler(conn, msg)

	// 更新统计
	latency := time.Since(startTime)
	actionStr := msg.Action.String()

	r.statsMu.Lock()
	if err != nil {
		r.stats.MessagesFailed++
		logger.Ctx(ctx).Error("Handler failed",
			zap.String("action", actionStr),
			zap.String("remote", conn.GetRemote()),
			zap.Duration("latency", latency),
			zap.Error(err))
	} else {
		r.stats.MessagesSucceeded++
		r.stats.HandlerStats[actionStr]++
		logger.Ctx(ctx).Debug("Message handled successfully",
			zap.String("action", actionStr),
			zap.String("remote", conn.GetRemote()),
			zap.Duration("latency", latency))
	}

	// 更新平均延迟
	if r.stats.AverageLatency == 0 {
		r.stats.AverageLatency = latency
	} else {
		r.stats.AverageLatency = (r.stats.AverageLatency + latency) / 2
	}
	r.statsMu.Unlock()

	return err
}

// GetStats 获取路由统计信息
func (r *StandardRouter) GetStats() *RouterStats {
	r.statsMu.RLock()
	defer r.statsMu.RUnlock()

	// 手动复制数据字段，不复制mutex
	handlerStats := make(map[string]int64)
	for k, v := range r.stats.HandlerStats {
		handlerStats[k] = v
	}

	return &RouterStats{
		HandlersRegistered: r.stats.HandlersRegistered,
		MessagesProcessed:  r.stats.MessagesProcessed,
		MessagesSucceeded:  r.stats.MessagesSucceeded,
		MessagesFailed:     r.stats.MessagesFailed,
		AverageLatency:     r.stats.AverageLatency,
		HandlerStats:       handlerStats,
		LastMessageTime:    r.stats.LastMessageTime,
	}
}

// GetHandlerCount 获取已注册的处理器数量
func (r *StandardRouter) GetHandlerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}

// ========== 内置处理器 ==========

// RegisterBuiltinHandlers 注册内置处理器
func (r *StandardRouter) RegisterBuiltinHandlers() {
	ctx := context.Background()

	// 注册ping处理器
	r.RegisterHandler(protocol.ActionPing, func(conn Connection, msg *protocol.Message) error {
		logger.Ctx(ctx).Debug("Received ping, sending pong",
			zap.String("remote", conn.GetRemote()))

		pongData := []byte("pong")
		return conn.SendResponse(protocol.ActionPong, msg.Id, pongData)
	})

	// 注册pong处理器
	r.RegisterHandler(protocol.ActionPong, func(conn Connection, msg *protocol.Message) error {
		logger.Ctx(ctx).Debug("Received pong",
			zap.String("remote", conn.GetRemote()))
		return nil
	})

	// 注册状态处理器
	r.RegisterHandler(protocol.ActionStatus, func(conn Connection, msg *protocol.Message) error {
		logger.Ctx(ctx).Debug("Received status request",
			zap.String("remote", conn.GetRemote()))

		statusData := []byte(fmt.Sprintf(`{"status":"ok","timestamp":%d}`, time.Now().Unix()))
		return conn.SendResponse(protocol.ActionStatus, msg.Id, statusData)
	})

	logger.Ctx(ctx).Info("Built-in handlers registered",
		zap.Int("count", 3))
}
