package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"datahive/pkg/logger"
	"datahive/pkg/protocol"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

// MessageHandler 客户端消息处理器
type MessageHandler func(msg *protocol.Message) error

// Client TCP客户端接口
type Client interface {
	Connect(address string) error
	Disconnect() error
	IsConnected() bool
	GetState() ClientState

	// 消息发送
	SendMessage(msg *protocol.Message) error
	SendRequest(action protocol.ActionType, data []byte) (*protocol.Message, error)
	SendRequestWithTimeout(action protocol.ActionType, data []byte, timeout time.Duration) (*protocol.Message, error)
	SendNotification(action protocol.ActionType, data []byte) error

	// 处理器注册
	RegisterHandler(action protocol.ActionType, handler MessageHandler) error
	UnregisterHandler(action protocol.ActionType) error

	// 回调设置
	SetOnConnected(fn func())
	SetOnDisconnected(fn func(error))
	SetOnError(fn func(error))

	// 状态获取
	GetStats() *ClientStats
}

// ClientState 客户端状态
type ClientState int32

const (
	StateDisconnected ClientState = iota
	StateConnecting
	StateConnected
	StateReconnecting
	StateClosed
)

func (s ClientState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ClientStats 客户端统计信息
type ClientStats struct {
	ConnectionAttempts int64         `json:"connection_attempts"`
	ConnectedAt        time.Time     `json:"connected_at"`
	LastActivity       time.Time     `json:"last_activity"`
	BytesSent          int64         `json:"bytes_sent"`
	BytesReceived      int64         `json:"bytes_received"`
	MessagesSent       int64         `json:"messages_sent"`
	MessagesReceived   int64         `json:"messages_received"`
	ErrorsCount        int64         `json:"errors_count"`
	ReconnectAttempts  int64         `json:"reconnect_attempts"`
	Uptime             time.Duration `json:"uptime"`
}

// PendingRequest 待处理的请求
type PendingRequest struct {
	RequestID  string
	Action     protocol.ActionType
	ResponseCh chan *protocol.Message
	ErrorCh    chan error
	Timeout    time.Time
}

// GNetClient gnet客户端实现
type GNetClient struct {
	config *Config

	// gnet客户端
	client *gnet.Client
	conn   gnet.Conn

	// 状态管理
	state int32 // ClientState

	// 连接管理
	address   string
	connected int32 // 0=false, 1=true

	// 消息处理
	handlers map[protocol.ActionType]MessageHandler
	mu       sync.RWMutex

	// 消息缓冲（处理分片消息）
	messageBuffer []byte
	bufferMu      sync.Mutex

	// 请求-响应管理
	pendingRequests map[string]*PendingRequest
	requestsMu      sync.RWMutex
	requestID       int64

	// 回调函数
	onConnected    func()
	onDisconnected func(error)
	onError        func(error)
	callbackMu     sync.RWMutex

	// 上下文管理
	ctx    context.Context
	cancel context.CancelFunc

	// 统计信息
	stats   *ClientStats
	statsMu sync.RWMutex
}

// NewGNetClient 创建 gnet 客户端
func NewGNetClient(cfg *Config) (*GNetClient, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &GNetClient{
		config:          cfg,
		handlers:        make(map[protocol.ActionType]MessageHandler),
		pendingRequests: make(map[string]*PendingRequest),
		ctx:             ctx,
		cancel:          cancel,
		stats:           &ClientStats{},
	}

	atomic.StoreInt32(&client.state, int32(StateDisconnected))

	return client, nil
}

// Connect 连接到服务器
func (c *GNetClient) Connect(address string) error {
	if !atomic.CompareAndSwapInt32(&c.state, int32(StateDisconnected), int32(StateConnecting)) {
		return fmt.Errorf("client is not in disconnected state")
	}

	c.address = address
	atomic.AddInt64(&c.stats.ConnectionAttempts, 1)

	logger.Ctx(context.Background()).Info("Connecting to server", zap.String("address", address))

	// 构建 gnet 客户端选项
	gnetOptions := []gnet.Option{
		gnet.WithTCPKeepAlive(c.config.TCPKeepAlive),
		gnet.WithReadBufferCap(c.config.GNet.ReadBufferCap),
		gnet.WithWriteBufferCap(c.config.GNet.WriteBufferCap),
	}

	// 设置 TCP NoDelay
	if c.config.TCPNoDelay {
		gnetOptions = append(gnetOptions, gnet.WithTCPNoDelay(gnet.TCPNoDelay))
	} else {
		gnetOptions = append(gnetOptions, gnet.WithTCPNoDelay(gnet.TCPDelay))
	}

	// 创建 gnet 客户端
	client, err := gnet.NewClient(c, gnetOptions...)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to create gnet client: %w", err)
	}

	c.client = client

	// 启动客户端并连接
	err = client.Start()
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to start gnet client: %w", err)
	}

	// 开始连接
	conn, err := client.Dial("tcp", address)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		client.Stop()
		return fmt.Errorf("failed to dial server: %w", err)
	}

	c.conn = conn

	// 启动请求超时清理协程
	go c.cleanupTimeoutRequests()

	return nil
}

// Disconnect 断开连接
func (c *GNetClient) Disconnect() error {
	currentState := atomic.LoadInt32(&c.state)
	if currentState == int32(StateClosed) || currentState == int32(StateDisconnected) {
		return nil
	}

	logger.Ctx(context.Background()).Info("Disconnecting from server")

	atomic.StoreInt32(&c.state, int32(StateClosed))
	atomic.StoreInt32(&c.connected, 0)

	// 关闭连接
	if c.conn != nil {
		c.conn.Close()
	}

	// 停止客户端
	if c.client != nil {
		c.client.Stop()
	}

	// 取消上下文
	if c.cancel != nil {
		c.cancel()
	}

	// 清理所有待处理的请求
	c.requestsMu.Lock()
	for _, req := range c.pendingRequests {
		select {
		case req.ErrorCh <- fmt.Errorf("client disconnected"):
		default:
		}
		close(req.ResponseCh)
		close(req.ErrorCh)
	}
	c.pendingRequests = make(map[string]*PendingRequest)
	c.requestsMu.Unlock()

	// 清理消息缓冲区
	c.bufferMu.Lock()
	c.messageBuffer = nil
	c.bufferMu.Unlock()

	return nil
}

// IsConnected 检查是否已连接
func (c *GNetClient) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

// GetState 获取当前状态
func (c *GNetClient) GetState() ClientState {
	return ClientState(atomic.LoadInt32(&c.state))
}

// SendMessage 发送消息
func (c *GNetClient) SendMessage(msg *protocol.Message) error {
	if !c.IsConnected() || c.conn == nil {
		return fmt.Errorf("client is not connected")
	}

	data, err := protocol.Marshal(msg)
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

	fullData := append(lengthPrefix, data...)

	// 发送数据
	err = c.conn.AsyncWrite(fullData, func(conn gnet.Conn, err error) error {
		if err != nil {
			atomic.AddInt64(&c.stats.ErrorsCount, 1)
		} else {
			atomic.AddInt64(&c.stats.MessagesSent, 1)
			atomic.AddInt64(&c.stats.BytesSent, int64(len(fullData)))
			c.statsMu.Lock()
			c.stats.LastActivity = time.Now()
			c.statsMu.Unlock()
		}
		return err
	})

	return err
}

// SendRequest 发送请求并等待响应
func (c *GNetClient) SendRequest(action protocol.ActionType, data []byte) (*protocol.Message, error) {
	return c.SendRequestWithTimeout(action, data, 30*time.Second)
}

// SendRequestWithTimeout 发送请求并等待响应（带超时）
func (c *GNetClient) SendRequestWithTimeout(action protocol.ActionType, data []byte, timeout time.Duration) (*protocol.Message, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("client is not connected")
	}

	// 生成请求ID
	requestID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), atomic.AddInt64(&c.requestID, 1))

	// 创建待处理请求
	req := &PendingRequest{
		RequestID:  requestID,
		Action:     action,
		ResponseCh: make(chan *protocol.Message, 1),
		ErrorCh:    make(chan error, 1),
		Timeout:    time.Now().Add(timeout),
	}

	// 注册待处理请求
	c.requestsMu.Lock()
	c.pendingRequests[requestID] = req
	c.requestsMu.Unlock()

	// 发送请求
	msg := protocol.NewRequestMessage(action, data)
	msg.Id = requestID

	err := c.SendMessage(msg)
	if err != nil {
		// 清理待处理请求
		c.requestsMu.Lock()
		delete(c.pendingRequests, requestID)
		c.requestsMu.Unlock()
		return nil, err
	}

	// 等待响应或超时
	select {
	case response := <-req.ResponseCh:
		c.requestsMu.Lock()
		delete(c.pendingRequests, requestID)
		c.requestsMu.Unlock()
		return response, nil
	case err := <-req.ErrorCh:
		c.requestsMu.Lock()
		delete(c.pendingRequests, requestID)
		c.requestsMu.Unlock()
		return nil, err
	case <-time.After(timeout):
		c.requestsMu.Lock()
		delete(c.pendingRequests, requestID)
		c.requestsMu.Unlock()
		return nil, fmt.Errorf("request timeout")
	case <-c.ctx.Done():
		c.requestsMu.Lock()
		delete(c.pendingRequests, requestID)
		c.requestsMu.Unlock()
		return nil, fmt.Errorf("client context cancelled")
	}
}

// SendNotification 发送通知
func (c *GNetClient) SendNotification(action protocol.ActionType, data []byte) error {
	msg := protocol.NewNotificationMessage(action, data)
	return c.SendMessage(msg)
}

// RegisterHandler 注册消息处理器
func (c *GNetClient) RegisterHandler(action protocol.ActionType, handler MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[action] = handler
	return nil
}

// UnregisterHandler 取消注册处理器
func (c *GNetClient) UnregisterHandler(action protocol.ActionType) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, action)
	return nil
}

// SetOnConnected 设置连接成功回调
func (c *GNetClient) SetOnConnected(callback func()) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onConnected = callback
}

// SetOnDisconnected 设置断开连接回调
func (c *GNetClient) SetOnDisconnected(callback func(error)) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onDisconnected = callback
}

// SetOnError 设置错误回调
func (c *GNetClient) SetOnError(callback func(error)) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onError = callback
}

// GetStats 获取统计信息
func (c *GNetClient) GetStats() *ClientStats {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()

	stats := *c.stats
	if !stats.ConnectedAt.IsZero() {
		stats.Uptime = time.Since(stats.ConnectedAt)
	}
	return &stats
}

// ========== gnet.EventHandler 接口实现 ==========

// OnBoot gnet 客户端启动
func (c *GNetClient) OnBoot(eng gnet.Engine) gnet.Action {
	ctx := context.Background()

	logger.Ctx(ctx).Debug("gnet client boot completed")
	return gnet.None
}

// OnShutdown gnet 客户端关闭
func (c *GNetClient) OnShutdown(eng gnet.Engine) {
	ctx := context.Background()

	logger.Ctx(ctx).Debug("gnet client shutdown completed")
}

// OnTick 定时器事件
func (c *GNetClient) OnTick() (time.Duration, gnet.Action) {
	return time.Minute, gnet.None
}

// OnOpen 连接建立
func (c *GNetClient) OnOpen(conn gnet.Conn) ([]byte, gnet.Action) {
	ctx := context.Background()

	logger.Ctx(ctx).Info("Connected to server", zap.String("remote", conn.RemoteAddr().String()))

	atomic.StoreInt32(&c.state, int32(StateConnected))
	atomic.StoreInt32(&c.connected, 1)

	c.statsMu.Lock()
	c.stats.ConnectedAt = time.Now()
	c.stats.LastActivity = time.Now()
	c.statsMu.Unlock()

	// 调用连接成功回调
	c.callbackMu.RLock()
	onConnected := c.onConnected
	c.callbackMu.RUnlock()

	if onConnected != nil {
		go onConnected()
	}

	return nil, gnet.None
}

// OnClose 连接关闭
func (c *GNetClient) OnClose(conn gnet.Conn, err error) gnet.Action {
	ctx := context.Background()

	logger.Ctx(ctx).Info("Disconnected from server", zap.Error(err))

	atomic.StoreInt32(&c.connected, 0)

	// 清理消息缓冲区
	c.bufferMu.Lock()
	c.messageBuffer = nil
	c.bufferMu.Unlock()

	// 调用断开连接回调
	c.callbackMu.RLock()
	onDisconnected := c.onDisconnected
	c.callbackMu.RUnlock()

	if onDisconnected != nil {
		go onDisconnected(err)
	}

	return gnet.None
}

// OnTraffic 处理接收到的数据
func (c *GNetClient) OnTraffic(conn gnet.Conn) gnet.Action {
	ctx := context.Background()

	// 读取数据
	data, err := conn.Next(-1)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to read data", zap.Error(err))
		atomic.AddInt64(&c.stats.ErrorsCount, 1)
		return gnet.Close
	}

	atomic.AddInt64(&c.stats.BytesReceived, int64(len(data)))

	// 处理数据
	if err := c.processReceivedData(data); err != nil {
		logger.Ctx(ctx).Error("Failed to process received data", zap.Error(err))
		atomic.AddInt64(&c.stats.ErrorsCount, 1)
		return gnet.Close
	}

	return gnet.None
}

// processReceivedData 处理接收到的数据
func (c *GNetClient) processReceivedData(data []byte) error {
	c.bufferMu.Lock()
	defer c.bufferMu.Unlock()

	// 将新数据追加到缓冲区
	c.messageBuffer = append(c.messageBuffer, data...)

	// 尝试处理缓冲区中的完整消息
	for len(c.messageBuffer) >= 4 {
		// 解析消息长度前缀
		messageLength := uint32(c.messageBuffer[0])<<24 | uint32(c.messageBuffer[1])<<16 | uint32(c.messageBuffer[2])<<8 | uint32(c.messageBuffer[3])
		totalMessageSize := int(4 + messageLength)

		// 检查是否有完整的消息
		if len(c.messageBuffer) < totalMessageSize {
			// 消息不完整，等待更多数据
			break
		}

		// 提取完整的消息
		messageData := c.messageBuffer[4:totalMessageSize]

		// 反序列化消息
		var msg protocol.Message
		if err := protocol.Unmarshal(messageData, &msg); err != nil {
			return fmt.Errorf("unmarshal message failed: %w", err)
		}

		// 从缓冲区中移除已处理的消息
		c.messageBuffer = c.messageBuffer[totalMessageSize:]

		// 更新统计
		atomic.AddInt64(&c.stats.MessagesReceived, 1)
		c.statsMu.Lock()
		c.stats.LastActivity = time.Now()
		c.statsMu.Unlock()

		// 处理消息
		if err := c.handleMessage(&msg); err != nil {
			return fmt.Errorf("handle message failed: %w", err)
		}
	}

	return nil
}

// handleMessage 处理消息
func (c *GNetClient) handleMessage(msg *protocol.Message) error {
	// 如果是响应消息，查找对应的待处理请求
	if msg.Type == protocol.TypeResponse || msg.Type == protocol.TypeError {
		c.requestsMu.RLock()
		req, exists := c.pendingRequests[msg.Id]
		c.requestsMu.RUnlock()

		if exists {
			if msg.Type == protocol.TypeError {
				errorMsg := "unknown error"
				// 错误信息现在在Payload中
				if len(msg.Payload) > 0 {
					errorMsg = string(msg.Payload)
				}
				select {
				case req.ErrorCh <- fmt.Errorf("server error: %s", errorMsg):
				default:
				}
			} else {
				select {
				case req.ResponseCh <- msg:
				default:
				}
			}
			return nil
		}
	}

	// 查找并调用处理器
	c.mu.RLock()
	handler, exists := c.handlers[msg.Action]
	c.mu.RUnlock()

	if exists {
		// 客户端消息处理器只需要消息参数
		return handler(msg)
	}

	// 没有找到处理器，记录日志但不返回错误
	logger.Ctx(context.Background()).Debug("No handler registered for action",
		zap.String("action", msg.Action.String()),
		zap.String("type", msg.Type.String()))

	return nil
}

// cleanupTimeoutRequests 清理超时的请求
func (c *GNetClient) cleanupTimeoutRequests() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.requestsMu.Lock()
			for requestID, req := range c.pendingRequests {
				if now.After(req.Timeout) {
					select {
					case req.ErrorCh <- fmt.Errorf("request timeout"):
					default:
					}
					close(req.ResponseCh)
					close(req.ErrorCh)
					delete(c.pendingRequests, requestID)
				}
			}
			c.requestsMu.Unlock()
		case <-c.ctx.Done():
			return
		}
	}
}

// CreateGNetClient 创建 gnet 客户端（工厂函数）
func CreateGNetClient(cfg *Config) (Client, error) {
	return NewGNetClient(cfg)
}

// DefaultGNetClient 创建带默认配置的 gnet 客户端
func DefaultGNetClient() (Client, error) {
	return NewGNetClient(DefaultConfig())
}
