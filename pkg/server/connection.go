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

	// 消息缓冲区 - 用于处理TCP流式传输
	buffer       []byte
	bufferMu     sync.Mutex
	expectedLen  uint32
	headerParsed bool

	// 统计
	stats *ConnectionStats
}

// NewGNetConnection 创建gnet连接
func NewGNetConnection(conn gnet.Conn, cfg *Config, router Router) *GNetConnection {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &GNetConnection{
		conn:     conn,
		config:   cfg,
		router:   router,
		handlers: make(map[pb.ActionType]MessageHandler),
		topics:   make(map[string]bool),
		buffer:   make([]byte, 0, 8192), // 初始化缓冲区
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

	builder := protocol.NewBuilder()
	data, err := builder.MarshalMessage(msg)
	if err != nil {
		atomic.AddInt64(&c.stats.ErrorsCount, 1)
		return fmt.Errorf("marshal message failed: %w", err)
	}

	// 添加长度前缀
	lengthPrefix := make([]byte, 4)
	lengthPrefix[0] = byte(len(data) >> 24)
	lengthPrefix[1] = byte(len(data) >> 16)
	lengthPrefix[2] = byte(len(data) >> 8)
	lengthPrefix[3] = byte(len(data))

	// 异步写入
	fullData := append(lengthPrefix, data...)
	err = c.conn.AsyncWrite(fullData, func(conn gnet.Conn, err error) error {
		if err != nil {
			atomic.AddInt64(&c.stats.ErrorsCount, 1)
		} else {
			atomic.AddInt64(&c.stats.MessagesWritten, 1)
			atomic.AddInt64(&c.stats.BytesWritten, int64(len(fullData)))
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

// OnTraffic 处理gnet流量事件 - 支持流式TCP消息解析
func (c *GNetConnection) OnTraffic(data []byte) error {
	if c.IsClosed() {
		return fmt.Errorf("connection closed")
	}

	// 更新统计
	atomic.AddInt64(&c.stats.BytesRead, int64(len(data)))
	c.stats.LastActivity = time.Now()

	c.bufferMu.Lock()
	defer c.bufferMu.Unlock()

	// 将新数据添加到缓冲区
	c.buffer = append(c.buffer, data...)

	// 循环处理缓冲区中的完整消息
	for {
		processed, err := c.processBuffer()
		if err != nil {
			atomic.AddInt64(&c.stats.ErrorsCount, 1)
			return err
		}
		if !processed {
			break // 没有更多完整消息可处理
		}
	}

	return nil
}

// processBuffer 处理缓冲区中的消息
func (c *GNetConnection) processBuffer() (bool, error) {
	// 如果还没有解析消息头，尝试解析消息长度
	if !c.headerParsed {
		if len(c.buffer) < 4 {
			return false, nil // 需要更多数据来读取消息头
		}

		// 解析消息长度（4字节大端序）
		c.expectedLen = uint32(c.buffer[0])<<24 |
			uint32(c.buffer[1])<<16 |
			uint32(c.buffer[2])<<8 |
			uint32(c.buffer[3])
		c.headerParsed = true

		// 检查消息长度是否合理（防止恶意攻击）
		if c.expectedLen > 1024*1024*10 { // 10MB 限制
			return false, fmt.Errorf("message too large: %d bytes", c.expectedLen)
		}
	}

	// 检查是否有完整的消息
	totalLen := 4 + c.expectedLen
	if len(c.buffer) < int(totalLen) {
		return false, nil // 需要更多数据
	}

	// 提取消息数据
	messageData := c.buffer[4:totalLen]

	// 解析消息
	var msg pb.Message
	builder := protocol.NewBuilder()
	if err := builder.UnmarshalMessage(messageData, &msg); err != nil {
		return false, fmt.Errorf("unmarshal message failed: %w", err)
	}

	atomic.AddInt64(&c.stats.MessagesRead, 1)

	// 移除已处理的数据
	c.buffer = c.buffer[totalLen:]
	c.headerParsed = false
	c.expectedLen = 0

	// 处理消息（在锁外执行以避免死锁）
	c.bufferMu.Unlock()
	err := c.ProcessMessage(&msg)
	c.bufferMu.Lock()

	if err != nil {
		return false, err
	}

	return true, nil // 成功处理了一条消息
}

// Close 关闭连接
func (c *GNetConnection) Close() error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		// 清理topics订阅 (断开连接自动清理)
		c.topicsMu.Lock()
		topicCount := len(c.topics)
		c.topics = make(map[string]bool) // 清空所有订阅
		c.topicsMu.Unlock()

		// 清理缓冲区
		c.bufferMu.Lock()
		c.buffer = nil
		c.headerParsed = false
		c.expectedLen = 0
		c.bufferMu.Unlock()

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
