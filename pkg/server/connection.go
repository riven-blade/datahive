package server

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol"
	"github.com/riven-blade/datahive/pkg/protocol/pb"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

// ConnectionStats 连接统计信息
type ConnectionStats struct {
	BytesRead       int64     `json:"bytes_read"`
	BytesWritten    int64     `json:"bytes_written"`
	MessagesRead    int64     `json:"messages_read"`
	MessagesWritten int64     `json:"messages_written"`
	ErrorsCount     int64     `json:"errors_count"`
	ConnectedAt     time.Time `json:"connected_at"`
	LastActivity    time.Time `json:"last_activity"`
}

// Connection 统一连接接口
type Connection interface {
	WriteMessage(msg *pb.Message) error
	ReadMessage() (*pb.Message, error)
	Close() error
	IsClosed() bool
	IsHealthy() bool
	GetRemote() string
	GetLocal() string

	SendRequest(action pb.ActionType, data []byte) (*pb.Message, error)
	SendResponse(action pb.ActionType, requestID string, data []byte) error
	SendNotification(action pb.ActionType, data []byte) error
	SendError(requestID string, code int32, message string) error

	RegisterHandler(action pb.ActionType, handler MessageHandler) error
	UnregisterHandler(action pb.ActionType) error

	Subscribe(topic string) error
	Unsubscribe(topic string) error
	HasTopic(topic string) bool

	GetStats() *ConnectionStats
}

// MessageHandler 消息处理器
type MessageHandler func(conn Connection, msg *pb.Message) error

// ========== GNet连接实现 ==========

// GNetConnection gnet高性能连接实现
type GNetConnection struct {
	conn   gnet.Conn
	config *Config

	// 服务器路由器
	router Router

	// 处理器
	handlers map[pb.ActionType]MessageHandler
	mu       sync.RWMutex

	// 订阅标签
	topics   map[string]bool
	topicsMu sync.RWMutex

	// 状态
	closed int32

	// 协议编解码器
	codec     *protocol.GNetCodec
	msgBuffer *protocol.MessageBuffer

	// 统计
	stats *ConnectionStats
}

// NewGNetConnection 创建gnet连接
func NewGNetConnection(conn gnet.Conn, cfg *Config, router Router) *GNetConnection {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// 创建协议编解码器
	codec := protocol.NewGNetCodec()
	msgBuffer := protocol.NewMessageBuffer(codec)

	return &GNetConnection{
		conn:      conn,
		config:    cfg,
		router:    router,
		handlers:  make(map[pb.ActionType]MessageHandler),
		topics:    make(map[string]bool),
		codec:     codec,
		msgBuffer: msgBuffer,
		stats: &ConnectionStats{
			ConnectedAt:  time.Now(),
			LastActivity: time.Now(),
		},
	}
}

// WriteMessage gnet写入消息
func (c *GNetConnection) WriteMessage(msg *pb.Message) error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	// 使用协议包编码消息
	data, err := c.codec.EncodeMessage(msg)
	if err != nil {
		atomic.AddInt64(&c.stats.ErrorsCount, 1)
		return fmt.Errorf("encode message failed: %w", err)
	}

	// 异步写入
	err = c.conn.AsyncWrite(data, func(conn gnet.Conn, err error) error {
		if err != nil {
			atomic.AddInt64(&c.stats.ErrorsCount, 1)
		} else {
			atomic.AddInt64(&c.stats.MessagesWritten, 1)
			atomic.AddInt64(&c.stats.BytesWritten, int64(len(data)))
			c.stats.LastActivity = time.Now()
		}
		return err
	})

	return err
}

// ReadMessage gnet不支持直接读取
func (c *GNetConnection) ReadMessage() (*pb.Message, error) {
	return nil, fmt.Errorf("use OnTraffic in gnet mode")
}

// OnTraffic 处理gnet流量事件 - 使用协议包解析
func (c *GNetConnection) OnTraffic(data []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	// 更新统计
	atomic.AddInt64(&c.stats.BytesRead, int64(len(data)))
	c.stats.LastActivity = time.Now()

	// 使用协议包处理接收到的数据
	messages, err := c.msgBuffer.ProcessData(data)
	if err != nil {
		atomic.AddInt64(&c.stats.ErrorsCount, 1)
		return fmt.Errorf("failed to process message data: %w", err)
	}

	// 处理解析出的所有消息
	for _, msg := range messages {
		atomic.AddInt64(&c.stats.MessagesRead, 1)
		if err := c.ProcessMessage(msg); err != nil {
			atomic.AddInt64(&c.stats.ErrorsCount, 1)
			return fmt.Errorf("failed to process message: %w", err)
		}
	}

	return nil
}

// Close 关闭连接
func (c *GNetConnection) Close() error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		// 清理topics订阅 (断开连接自动清理)
		c.topicsMu.Lock()
		topicCount := len(c.topics)
		c.topics = make(map[string]bool) // 清空所有订阅
		c.topicsMu.Unlock()

		// 清理协议缓冲区
		if c.msgBuffer != nil {
			c.msgBuffer.Clear()
		}

		if topicCount > 0 {
			// 记录日志
			logger.Debug("连接关闭，自动清理topics",
				zap.String("remote", c.GetRemote()),
				zap.Int("topics_cleared", topicCount))
		}

		return c.conn.Close()
	}
	return nil
}

// IsClosed 检查是否已关闭
func (c *GNetConnection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// IsHealthy 检查是否健康
func (c *GNetConnection) IsHealthy() bool {
	return !c.IsClosed()
}

// GetRemote 获取远程地址
func (c *GNetConnection) GetRemote() string {
	if c == nil || c.conn == nil {
		return "unknown"
	}

	if addr := c.conn.RemoteAddr(); addr != nil {
		return addr.String()
	}

	return "unknown"
}

// GetLocal 获取本地地址
func (c *GNetConnection) GetLocal() string {
	if c == nil || c.conn == nil {
		return "unknown"
	}

	if addr := c.conn.LocalAddr(); addr != nil {
		return addr.String()
	}

	return "unknown"
}

// SendRequest 发送请求
func (c *GNetConnection) SendRequest(action pb.ActionType, data []byte) (*pb.Message, error) {
	builder := protocol.NewBuilder()
	msg, err := builder.NewRequest(protocol.GenerateID(), action, nil)
	if err != nil {
		return nil, err
	}
	msg.Data = data

	err = c.WriteMessage(msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// SendResponse 发送响应
func (c *GNetConnection) SendResponse(action pb.ActionType, requestID string, data []byte) error {
	builder := protocol.NewBuilder()
	msg, err := builder.NewResponse(requestID, action, nil)
	if err != nil {
		return err
	}
	msg.Data = data
	return c.WriteMessage(msg)
}

// SendNotification 发送通知
func (c *GNetConnection) SendNotification(action pb.ActionType, data []byte) error {
	builder := protocol.NewBuilder()
	msg, err := builder.NewNotification(action, nil)
	if err != nil {
		return err
	}
	msg.Data = data
	return c.WriteMessage(msg)
}

// SendError 发送错误
func (c *GNetConnection) SendError(requestID string, code int32, message string) error {
	builder := protocol.NewBuilder()
	msg, err := builder.NewError(requestID, code, message, "")
	if err != nil {
		return err
	}
	return c.WriteMessage(msg)
}

// RegisterHandler 注册处理器
func (c *GNetConnection) RegisterHandler(action pb.ActionType, handler MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[action] = handler
	return nil
}

// UnregisterHandler 取消注册处理器
func (c *GNetConnection) UnregisterHandler(action pb.ActionType) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, action)
	return nil
}

// Subscribe 订阅主题
func (c *GNetConnection) Subscribe(topic string) error {
	c.topicsMu.Lock()
	defer c.topicsMu.Unlock()
	c.topics[topic] = true
	return nil
}

// Unsubscribe 取消订阅主题
func (c *GNetConnection) Unsubscribe(topic string) error {
	c.topicsMu.Lock()
	defer c.topicsMu.Unlock()
	delete(c.topics, topic)
	return nil
}

// HasTopic 检查是否订阅了主题
func (c *GNetConnection) HasTopic(topic string) bool {
	c.topicsMu.RLock()
	defer c.topicsMu.RUnlock()
	return c.topics[topic]
}

// GetStats 获取统计信息
func (c *GNetConnection) GetStats() *ConnectionStats {
	return c.stats
}

// ProcessMessage 处理消息
func (c *GNetConnection) ProcessMessage(msg *pb.Message) error {
	// 优先使用服务器路由器
	if c.router != nil {
		err := c.router.RouteMessage(c, msg)
		if err == nil || !errors.Is(err, ErrNoHandler) {
			return err
		}
	}

	// 回退到连接级别的处理器
	c.mu.RLock()
	handler, exists := c.handlers[msg.Action]
	c.mu.RUnlock()

	if exists {
		return handler(c, msg)
	}

	return ErrNoHandler // 没有处理器，忽略消息
}
