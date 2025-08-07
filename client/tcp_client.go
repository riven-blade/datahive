package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"
	"github.com/riven-blade/datahive/pkg/protocol"
	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"google.golang.org/protobuf/proto"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

// MessageHandler 消息处理器
type MessageHandler func(msg *pb.Message) error

// Client TCP客户端接口
type Client interface {
	Connect(address string) error
	Disconnect() error
	IsConnected() bool

	// 消息发送
	SendMessage(msg *pb.Message) error
	SendRequest(action pb.ActionType, data []byte) (*pb.Message, error)
	SendRequestWithTimeout(action pb.ActionType, data []byte, timeout time.Duration) (*pb.Message, error)

	// 处理器注册
	RegisterHandler(action pb.ActionType, handler MessageHandler) error
	UnregisterHandler(action pb.ActionType) error

	// 回调设置
	SetOnConnected(fn func())
	SetOnDisconnected(fn func(error))
	SetOnError(fn func(error))

	// 状态获取
	GetStats() *ClientStats

	// 重连控制
	StopAutoReconnect()
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
	ConnectedAt       time.Time
	LastActivity      time.Time
	BytesSent         int64
	BytesReceived     int64
	MessagesSent      int64
	MessagesReceived  int64
	ErrorsCount       int64
	ReconnectAttempts int64
}

// PendingRequest 待处理的请求
type PendingRequest struct {
	RequestID  string
	Action     pb.ActionType
	ResponseCh chan *pb.Message
	ErrorCh    chan error
	Timeout    time.Time
}

// TCPClient TCP客户端实现
type TCPClient struct {
	config *Config
	logger *logger.MLogger

	// gnet客户端
	gnetClient *gnet.Client
	conn       gnet.Conn

	// 状态管理
	state   int32 // ClientState
	address string

	// 消息处理
	handlers        map[pb.ActionType]MessageHandler
	pendingRequests map[string]*PendingRequest
	requestID       int64
	mu              sync.RWMutex

	// 回调函数
	onConnected    func()
	onDisconnected func(error)
	onError        func(error)
	callbackMu     sync.RWMutex

	// 生命周期
	ctx    context.Context
	cancel context.CancelFunc

	// 重连管理
	reconnectTicker   *time.Ticker
	reconnectAttempts int64
	lastAddress       string

	// 统计信息
	stats   *ClientStats
	statsMu sync.RWMutex

	// 协议编解码器
	codec     *protocol.GNetCodec
	msgBuffer *protocol.MessageBuffer
}

// NewTCPClient 创建TCP客户端
func NewTCPClient(cfg *Config) (Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// 创建协议编解码器
	codec := protocol.NewGNetCodec()
	msgBuffer := protocol.NewMessageBuffer(codec)

	client := &TCPClient{
		config:          cfg,
		logger:          cfg.Logger,
		handlers:        make(map[pb.ActionType]MessageHandler),
		pendingRequests: make(map[string]*PendingRequest),
		stats:           &ClientStats{},
		ctx:             ctx,
		cancel:          cancel,
		codec:           codec,
		msgBuffer:       msgBuffer,
	}

	atomic.StoreInt32(&client.state, int32(StateDisconnected))
	return client, nil
}

// Connect 连接到服务器
func (c *TCPClient) Connect(address string) error {
	if !atomic.CompareAndSwapInt32(&c.state, int32(StateDisconnected), int32(StateConnecting)) {
		return fmt.Errorf("client is not in disconnected state")
	}

	c.address = address
	c.lastAddress = address
	atomic.StoreInt64(&c.reconnectAttempts, 0) // 重置重连计数器
	c.logger.Debug("Connecting to server", zap.String("address", address))

	// 创建gnet客户端
	gnetClient, err := gnet.NewClient(c)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to create gnet client: %w", err)
	}

	c.gnetClient = gnetClient

	// 启动客户端
	err = c.gnetClient.Start()
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to start gnet client: %w", err)
	}

	// 连接到服务器
	conn, err := c.gnetClient.Dial("tcp", address)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		c.gnetClient.Stop()
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	atomic.StoreInt32(&c.state, int32(StateConnected))

	c.statsMu.Lock()
	c.stats.ConnectedAt = time.Now()
	c.stats.LastActivity = time.Now()
	c.statsMu.Unlock()

	c.logger.Info("Connected to server", zap.String("address", address))

	// 触发连接回调
	c.callbackMu.RLock()
	onConnected := c.onConnected
	c.callbackMu.RUnlock()

	if onConnected != nil {
		go onConnected()
	}

	return nil
}

// Disconnect 断开连接
func (c *TCPClient) Disconnect() error {
	atomic.StoreInt32(&c.state, int32(StateClosed))

	c.logger.Debug("Disconnecting from server")

	// 关闭连接
	if c.conn != nil {
		c.conn.Close()
	}

	if c.gnetClient != nil {
		c.gnetClient.Stop()
	}

	// 停止后台任务
	c.cancel()

	c.logger.Info("Client disconnected")
	return nil
}

// IsConnected 检查连接状态
func (c *TCPClient) IsConnected() bool {
	return atomic.LoadInt32(&c.state) == int32(StateConnected)
}

// SendMessage 发送消息
func (c *TCPClient) SendMessage(msg *pb.Message) error {
	if !c.IsConnected() {
		return fmt.Errorf("not connected")
	}

	c.logger.Debug("Sending message",
		zap.String("action", msg.Action.String()),
		zap.String("id", msg.Id),
		zap.String("type", msg.Type.String()),
		zap.Int("data_size", len(msg.Data)))

	// 使用协议包编码消息
	data, err := c.codec.EncodeMessage(msg)
	if err != nil {
		c.logger.Error("Failed to encode message",
			zap.Error(err),
			zap.String("action", msg.Action.String()))
		return fmt.Errorf("failed to encode message: %w", err)
	}

	// 发送编码后的数据
	_, err = c.conn.Write(data)
	if err != nil {
		c.logger.Error("Failed to send message",
			zap.Error(err),
			zap.String("action", msg.Action.String()))
		return fmt.Errorf("failed to send message: %w", err)
	}

	c.statsMu.Lock()
	c.stats.MessagesSent++
	c.stats.BytesSent += int64(len(data))
	c.stats.LastActivity = time.Now()
	c.statsMu.Unlock()

	c.logger.Debug("Message sent successfully",
		zap.String("action", msg.Action.String()),
		zap.Int("encoded_bytes", len(data)))

	return nil
}

// SendRequest 发送请求并等待响应
func (c *TCPClient) SendRequest(action pb.ActionType, data []byte) (*pb.Message, error) {
	return c.SendRequestWithTimeout(action, data, c.config.RequestTimeout)
}

// SendRequestWithTimeout 发送请求并等待响应(带超时)
func (c *TCPClient) SendRequestWithTimeout(action pb.ActionType, data []byte, timeout time.Duration) (*pb.Message, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}

	// 生成请求ID
	requestID := fmt.Sprintf("%d", atomic.AddInt64(&c.requestID, 1))

	// 创建待处理请求
	req := &PendingRequest{
		RequestID:  requestID,
		Action:     action,
		ResponseCh: make(chan *pb.Message, 1),
		ErrorCh:    make(chan error, 1),
		Timeout:    time.Now().Add(timeout),
	}

	c.mu.Lock()
	c.pendingRequests[requestID] = req
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pendingRequests, requestID)
		c.mu.Unlock()
		close(req.ResponseCh)
		close(req.ErrorCh)
	}()

	// 构造请求消息
	msg := &pb.Message{
		Type:      pb.MessageType_REQUEST,
		Action:    action,
		Id:        requestID,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}

	// 发送请求
	if err := c.SendMessage(msg); err != nil {
		return nil, err
	}

	// 等待响应
	select {
	case resp := <-req.ResponseCh:
		if resp.Type == pb.MessageType_ERROR {
			var errorMsg pb.Error
			if err := proto.Unmarshal(resp.Data, &errorMsg); err == nil {
				return nil, fmt.Errorf("server error: %s (code: %d)", errorMsg.Message, errorMsg.Code)
			}
			return nil, fmt.Errorf("server error")
		}
		return resp, nil
	case err := <-req.ErrorCh:
		return nil, err
	case <-time.After(timeout):
		return nil, fmt.Errorf("request timeout")
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}

// RegisterHandler 注册消息处理器
func (c *TCPClient) RegisterHandler(action pb.ActionType, handler MessageHandler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[action] = handler
	return nil
}

// UnregisterHandler 取消注册消息处理器
func (c *TCPClient) UnregisterHandler(action pb.ActionType) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, action)
	return nil
}

// SetOnConnected 设置连接回调
func (c *TCPClient) SetOnConnected(fn func()) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onConnected = fn
}

// SetOnDisconnected 设置断开连接回调
func (c *TCPClient) SetOnDisconnected(fn func(error)) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onDisconnected = fn
}

// SetOnError 设置错误回调
func (c *TCPClient) SetOnError(fn func(error)) {
	c.callbackMu.Lock()
	defer c.callbackMu.Unlock()
	c.onError = fn
}

// GetStats 获取统计信息
func (c *TCPClient) GetStats() *ClientStats {
	c.statsMu.RLock()
	defer c.statsMu.RUnlock()

	stats := *c.stats
	return &stats
}

// =============================================================================
// gnet.EventHandler Interface Implementation
// =============================================================================

func (c *TCPClient) OnBoot(eng gnet.Engine) gnet.Action {
	c.logger.Debug("Client boot")
	return gnet.None
}

func (c *TCPClient) OnShutdown(eng gnet.Engine) {
	c.logger.Debug("Client shutdown")
}

func (c *TCPClient) OnOpen(gc gnet.Conn) ([]byte, gnet.Action) {
	c.logger.Debug("TCP connection opened",
		zap.String("local_addr", gc.LocalAddr().String()),
		zap.String("remote_addr", gc.RemoteAddr().String()))
	return nil, gnet.None
}

func (c *TCPClient) OnClose(gc gnet.Conn, err error) gnet.Action {
	c.logger.Debug("TCP connection closed",
		zap.Error(err),
		zap.String("remote_addr", gc.RemoteAddr().String()))

	if atomic.LoadInt32(&c.state) != int32(StateClosed) {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))

		// 启动自动重连（如果启用）
		if c.config.EnableAutoReconnect && c.lastAddress != "" {
			go c.startAutoReconnect(err)
		} else {
			// 触发断开连接回调
			c.callbackMu.RLock()
			onDisconnected := c.onDisconnected
			c.callbackMu.RUnlock()

			if onDisconnected != nil {
				go onDisconnected(err)
			}
		}
	}

	return gnet.None
}

func (c *TCPClient) OnTraffic(gc gnet.Conn) gnet.Action {
	// 读取数据
	data, err := gc.Next(-1)
	if err != nil {
		c.notifyError(fmt.Errorf("failed to read data: %w", err))
		return gnet.None
	}

	// 处理流式TCP消息
	if err := c.onTrafficData(data); err != nil {
		c.notifyError(fmt.Errorf("failed to process traffic data: %w", err))
		return gnet.None
	}

	return gnet.None
}

// onTrafficData 处理接收到的数据，使用协议包解析
func (c *TCPClient) onTrafficData(data []byte) error {
	//c.logger.Debug("Processing received data",
	//	zap.Int("bytes_received", len(data)))

	// 使用协议包处理接收到的数据
	messages, err := c.msgBuffer.ProcessData(data)
	if err != nil {
		c.logger.Error("Failed to process message data",
			zap.Error(err),
			zap.Int("data_length", len(data)))
		return fmt.Errorf("failed to process message data: %w", err)
	}

	//c.logger.Debug("Parsed messages from data",
	//	zap.Int("message_count", len(messages)),
	//	zap.Int("data_bytes", len(data)))

	// 处理解析出的所有消息
	for _, msg := range messages {
		c.handleMessage(msg)
	}

	c.statsMu.Lock()
	c.stats.BytesReceived += int64(len(data))
	c.stats.MessagesReceived += int64(len(messages))
	c.stats.LastActivity = time.Now()
	c.statsMu.Unlock()

	return nil
}

// OnTick gnet.EventHandler interface requirement
func (c *TCPClient) OnTick() (delay time.Duration, action gnet.Action) {
	return time.Second, gnet.None
}

// =============================================================================
// Internal Methods
// =============================================================================

// handleMessage 处理收到的消息
func (c *TCPClient) handleMessage(msg *pb.Message) {
	//now := time.Now()
	//receiveTime := now.UnixMilli()

	// 计算消息延迟（接收时间 - 消息创建时间）
	//var latencyMs int64
	//if msg.Timestamp > 0 {
	//	latencyMs = receiveTime - msg.Timestamp
	//}

	//c.logger.Debug("TCP Client received message",
	//	zap.String("action", msg.Action.String()),
	//	zap.String("id", msg.Id),
	//	zap.Int("data_size", len(msg.Data)),
	//	zap.Int64("msg_timestamp", msg.Timestamp),
	//	zap.Int64("receive_timestamp", receiveTime),
	//	zap.Int64("latency_ms", latencyMs))

	// 如果是响应消息，处理待处理请求
	if msg.Id != "" {
		c.mu.RLock()
		req, exists := c.pendingRequests[msg.Id]
		c.mu.RUnlock()

		if exists {
			c.logger.Debug("Processing response message", zap.String("request_id", msg.Id))
			select {
			case req.ResponseCh <- msg:
			default:
				c.logger.Warn("Response channel full", zap.String("request_id", msg.Id))
			}
			return
		}
	}

	// 处理订阅消息
	c.mu.RLock()
	handler, exists := c.handlers[msg.Action]
	c.mu.RUnlock()

	//c.logger.Debug("Looking for message handler",
	//	zap.String("action", msg.Action.String()),
	//	zap.Bool("handler_exists", exists))

	if exists {
		//c.logger.Debug("Calling message handler", zap.String("action", msg.Action.String()))
		if err := handler(msg); err != nil {
			c.logger.Error("Handler execution failed",
				zap.String("action", msg.Action.String()),
				zap.Error(err))
			c.notifyError(fmt.Errorf("handler error: %w", err))
		}
	} else {
		c.logger.Warn("No handler found for action", zap.String("action", msg.Action.String()))
	}
}

// notifyError 通知错误
func (c *TCPClient) notifyError(err error) {
	c.statsMu.Lock()
	c.stats.ErrorsCount++
	c.statsMu.Unlock()

	c.callbackMu.RLock()
	onError := c.onError
	c.callbackMu.RUnlock()

	if onError != nil {
		go onError(err)
	}

	c.logger.Error("Client error", zap.Error(err))
}

// startAutoReconnect 启动自动重连
func (c *TCPClient) startAutoReconnect(disconnectErr error) {
	attempts := atomic.LoadInt64(&c.reconnectAttempts)
	maxAttempts := int64(c.config.MaxReconnectAttempts)

	// 检查是否超过最大重连次数
	if maxAttempts > 0 && attempts >= maxAttempts {
		c.logger.Error("Maximum reconnect attempts reached",
			zap.Int64("attempts", attempts),
			zap.Int("max_attempts", c.config.MaxReconnectAttempts))

		// 触发断开连接回调
		c.callbackMu.RLock()
		onDisconnected := c.onDisconnected
		c.callbackMu.RUnlock()

		if onDisconnected != nil {
			go onDisconnected(disconnectErr)
		}
		return
	}

	// 更新状态为重连中
	if !atomic.CompareAndSwapInt32(&c.state, int32(StateDisconnected), int32(StateReconnecting)) {
		return // 状态已改变，可能已经在重连或已连接
	}

	atomic.AddInt64(&c.reconnectAttempts, 1)
	currentAttempts := atomic.LoadInt64(&c.reconnectAttempts)

	c.logger.Debug("Starting reconnection attempt",
		zap.Int64("attempt", currentAttempts),
		zap.Int("max_attempts", c.config.MaxReconnectAttempts),
		zap.String("address", c.lastAddress))

	// 等待重连间隔
	time.Sleep(c.config.ReconnectInterval)

	// 检查是否被取消
	select {
	case <-c.ctx.Done():
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return
	default:
	}

	// 尝试重连
	if err := c.attemptReconnect(); err != nil {
		c.logger.Debug("Reconnection attempt failed",
			zap.Error(err),
			zap.Int64("attempt", currentAttempts))

		// 继续尝试重连
		go c.startAutoReconnect(disconnectErr)
	} else {
		c.logger.Info("Successfully reconnected",
			zap.Int64("attempts", currentAttempts),
			zap.String("address", c.lastAddress))

		// 重置重连计数器
		atomic.StoreInt64(&c.reconnectAttempts, 0)

		// 触发连接成功回调
		c.callbackMu.RLock()
		onConnected := c.onConnected
		c.callbackMu.RUnlock()

		if onConnected != nil {
			go onConnected()
		}
	}
}

// attemptReconnect 尝试重新连接
func (c *TCPClient) attemptReconnect() error {
	// 清理旧的连接资源
	if c.gnetClient != nil {
		c.gnetClient.Stop()
		c.gnetClient = nil
	}

	// 创建新的gnet客户端
	gnetClient, err := gnet.NewClient(c)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to create gnet client: %w", err)
	}

	c.gnetClient = gnetClient

	// 启动客户端
	err = c.gnetClient.Start()
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		return fmt.Errorf("failed to start gnet client: %w", err)
	}

	// 连接到服务器
	conn, err := c.gnetClient.Dial("tcp", c.lastAddress)
	if err != nil {
		atomic.StoreInt32(&c.state, int32(StateDisconnected))
		c.gnetClient.Stop()
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.conn = conn
	atomic.StoreInt32(&c.state, int32(StateConnected))
	c.logger.Debug("Reconnected successfully", zap.String("address", c.lastAddress))

	return nil
}

// StopAutoReconnect 停止自动重连
func (c *TCPClient) StopAutoReconnect() {
	c.config.EnableAutoReconnect = false
	if c.reconnectTicker != nil {
		c.reconnectTicker.Stop()
		c.reconnectTicker = nil
	}
}
