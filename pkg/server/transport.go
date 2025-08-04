package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"datahive/pkg/logger"
	"datahive/pkg/protocol"
	"datahive/pkg/protocol/pb"

	"github.com/panjf2000/gnet/v2"
	"go.uber.org/zap"
)

// TransportStats 传输层统计信息
type TransportStats struct {
	ConnectionsAccepted int64     `json:"connections_accepted"`
	ConnectionsClosed   int64     `json:"connections_closed"`
	CurrentConnections  int64     `json:"current_connections"`
	MessagesReceived    int64     `json:"messages_received"`
	MessagesSent        int64     `json:"messages_sent"`
	BytesReceived       int64     `json:"bytes_received"`
	BytesSent           int64     `json:"bytes_sent"`
	ErrorsCount         int64     `json:"errors_count"`
	StartTime           time.Time `json:"start_time"`
	Uptime              string    `json:"uptime"`
}

// Transport 传输层接口
type Transport interface {
	Start() error
	Stop() error
	IsRunning() bool
	RegisterHandler(action pb.ActionType, handler MessageHandler)
	UnregisterHandler(action pb.ActionType)
	Broadcast(action pb.ActionType, data []byte, topic string) error
	GetStats() *TransportStats
	GetConnectionCount() int
	GetAllConnections() []Connection
	SetRouter(router Router)
}

type GNetTransport struct {
	config *Config
	addr   string

	// gnet引擎
	engine gnet.Engine

	// 连接管理
	connections   map[string]*GNetConnection
	connectionsMu sync.RWMutex

	// 消息路由
	router Router

	// 状态管理
	state     int32 // 0=stopped, 1=running
	startTime time.Time

	// 上下文
	ctx    context.Context
	cancel context.CancelFunc

	// 统计信息
	stats *TransportStats
}

func NewGNetTransport(addr string, cfg *Config) *GNetTransport {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &GNetTransport{
		config:      cfg,
		addr:        addr,
		connections: make(map[string]*GNetConnection),
		router:      NewRouter(),
		ctx:         ctx,
		cancel:      cancel,
		stats:       &TransportStats{},
	}
}

// Start 启动gnet传输层
func (t *GNetTransport) Start() error {
	if !atomic.CompareAndSwapInt32(&t.state, 0, 1) {
		return fmt.Errorf("transport already running")
	}

	t.startTime = time.Now()
	t.stats.StartTime = t.startTime

	logger.Ctx(t.ctx).Info("Starting gnet high-performance transport",
		zap.String("addr", t.addr),
		zap.Int("max_connections", t.config.MaxConnections))

	// 构建gnet选项
	gnetOptions := []gnet.Option{
		gnet.WithMulticore(t.config.GNet.Multicore),
		gnet.WithReusePort(t.config.GNet.ReusePort),
		gnet.WithTCPKeepAlive(t.config.TCPKeepAlive),
		gnet.WithLoadBalancing(gnet.LoadBalancing(t.config.GNet.LoadBalancing)),
		gnet.WithNumEventLoop(t.config.GNet.NumEventLoop),
		gnet.WithReadBufferCap(t.config.GNet.ReadBufferCap),
		gnet.WithWriteBufferCap(t.config.GNet.WriteBufferCap),
	}

	// 根据配置设置TCP NoDelay
	if t.config.TCPNoDelay {
		gnetOptions = append(gnetOptions, gnet.WithTCPNoDelay(gnet.TCPNoDelay))
	} else {
		gnetOptions = append(gnetOptions, gnet.WithTCPNoDelay(gnet.TCPDelay))
	}

	// 设置Socket缓冲区（如果配置了）
	if t.config.GNet.SocketRecvBuffer > 0 {
		gnetOptions = append(gnetOptions, gnet.WithSocketRecvBuffer(t.config.GNet.SocketRecvBuffer))
	}
	if t.config.GNet.SocketSendBuffer > 0 {
		gnetOptions = append(gnetOptions, gnet.WithSocketSendBuffer(t.config.GNet.SocketSendBuffer))
	}

	// 启动gnet服务器
	addr := "tcp://" + t.addr
	err := gnet.Run(t, addr, gnetOptions...)

	if err != nil {
		atomic.StoreInt32(&t.state, 0)
		return fmt.Errorf("failed to start gnet transport: %w", err)
	}

	return nil
}

// Stop 停止gnet传输层
func (t *GNetTransport) Stop() error {
	if !atomic.CompareAndSwapInt32(&t.state, 1, 0) {
		return fmt.Errorf("transport not running")
	}

	logger.Ctx(t.ctx).Info("Stopping gnet transport...")

	// 关闭所有连接
	t.connectionsMu.Lock()
	for _, conn := range t.connections {
		conn.Close()
	}
	t.connections = make(map[string]*GNetConnection)
	t.connectionsMu.Unlock()

	if t.cancel != nil {
		t.cancel()
	}

	return t.engine.Stop(t.ctx)
}

// IsRunning 检查是否运行中
func (t *GNetTransport) IsRunning() bool {
	return atomic.LoadInt32(&t.state) == 1
}

// ========== gnet.EventHandler 接口实现 ==========

// OnBoot gnet启动
func (t *GNetTransport) OnBoot(eng gnet.Engine) gnet.Action {
	t.engine = eng
	logger.Ctx(t.ctx).Info("gnet transport boot completed", zap.String("addr", t.addr))
	return gnet.None
}

// OnShutdown gnet关闭
func (t *GNetTransport) OnShutdown(eng gnet.Engine) {
	logger.Ctx(t.ctx).Info("gnet transport shutdown completed")
}

// OnTick 定时器事件
func (t *GNetTransport) OnTick() (time.Duration, gnet.Action) {
	return 30 * time.Second, gnet.None
}

// OnOpen 新连接
func (t *GNetTransport) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	// 检查连接数限制
	currentConnections := atomic.LoadInt64(&t.stats.CurrentConnections)
	if currentConnections >= int64(t.config.MaxConnections) {
		logger.Ctx(t.ctx).Warn("Connection limit reached",
			zap.String("remote", c.RemoteAddr().String()),
			zap.Int64("current", currentConnections),
			zap.Int("max", t.config.MaxConnections))
		return nil, gnet.Close
	}

	// 创建连接
	conn := NewGNetConnection(c, t.config, t.router)
	remote := c.RemoteAddr().String()

	// 注册连接
	t.connectionsMu.Lock()
	t.connections[remote] = conn
	t.connectionsMu.Unlock()

	c.SetContext(conn)

	// 更新统计
	atomic.AddInt64(&t.stats.ConnectionsAccepted, 1)
	atomic.AddInt64(&t.stats.CurrentConnections, 1)

	logger.Ctx(t.ctx).Info("New gnet connection accepted", zap.String("remote", remote))

	return nil, gnet.None
}

// OnClose 连接关闭
func (t *GNetTransport) OnClose(c gnet.Conn, err error) gnet.Action {
	remote := c.RemoteAddr().String()

	// 移除连接
	t.connectionsMu.Lock()
	if conn, exists := t.connections[remote]; exists {
		conn.Close()
		delete(t.connections, remote)
	}
	t.connectionsMu.Unlock()

	// 更新统计
	atomic.AddInt64(&t.stats.ConnectionsClosed, 1)
	atomic.AddInt64(&t.stats.CurrentConnections, -1)

	logger.Ctx(t.ctx).Info("gnet connection closed", zap.String("remote", remote))

	return gnet.None
}

// OnTraffic 处理流量
func (t *GNetTransport) OnTraffic(c gnet.Conn) gnet.Action {
	conn, ok := c.Context().(*GNetConnection)
	if !ok {
		logger.Ctx(t.ctx).Error("Invalid gnet connection context")
		return gnet.Close
	}

	// 读取数据
	data, err := c.Next(-1)
	if err != nil {
		logger.Ctx(t.ctx).Error("Failed to read gnet data", zap.Error(err))
		return gnet.Close
	}

	atomic.AddInt64(&t.stats.BytesReceived, int64(len(data)))

	// 处理流量
	if err := conn.OnTraffic(data); err != nil {
		logger.Ctx(t.ctx).Error("Failed to process gnet traffic", zap.Error(err))
		return gnet.Close
	}

	atomic.AddInt64(&t.stats.MessagesReceived, 1)

	return gnet.None
}

// RegisterHandler 注册处理器
func (t *GNetTransport) RegisterHandler(action pb.ActionType, handler MessageHandler) {
	t.router.RegisterHandler(action, handler)
}

// UnregisterHandler 取消注册处理器
func (t *GNetTransport) UnregisterHandler(action pb.ActionType) {
	t.router.UnregisterHandler(action)
}

// Broadcast 广播消息
func (t *GNetTransport) Broadcast(action pb.ActionType, data []byte, topic string) error {
	builder := protocol.NewBuilder()
	msg, err := builder.NewNotification(action, nil)
	if err != nil {
		return err
	}
	msg.Data = data

	t.connectionsMu.RLock()
	var connections []*GNetConnection
	totalConnections := len(t.connections)

	// 如果指定了topic，只向订阅了该topic的连接发送
	if topic != "" {
		for _, conn := range t.connections {
			if conn.HasTopic(topic) {
				connections = append(connections, conn)
			}
		}
	} else {
		// 如果topic为空，向所有连接广播
		connections = make([]*GNetConnection, 0, totalConnections)
		for _, conn := range t.connections {
			connections = append(connections, conn)
		}
	}
	t.connectionsMu.RUnlock()

	logger.Ctx(t.ctx).Debug("开始广播消息",
		zap.String("action", action.String()),
		zap.String("topic", topic),
		zap.Int("target_connections", len(connections)),
		zap.Int("total_connections", totalConnections),
		zap.Int("data_size", len(data)))

	if len(connections) == 0 {
		if topic != "" {
			logger.Ctx(t.ctx).Debug("没有连接订阅该topic",
				zap.String("action", action.String()),
				zap.String("topic", topic))
		} else {
			logger.Ctx(t.ctx).Warn("没有活跃连接，无法广播消息",
				zap.String("action", action.String()))
		}
		return nil // 没有连接不算错误
	}

	var errorCount int64
	var wg sync.WaitGroup

	for _, conn := range connections {
		if conn == nil {
			continue // 跳过 nil 连接
		}

		wg.Add(1)
		go func(c *GNetConnection) {
			defer wg.Done()

			// 再次检查连接是否有效
			if c == nil || c.IsClosed() {
				return
			}

			if err := c.WriteMessage(msg); err != nil {
				atomic.AddInt64(&errorCount, 1)
				logger.Ctx(t.ctx).Warn("向连接广播消息失败",
					zap.String("action", action.String()),
					zap.String("topic", topic),
					zap.String("remote", c.GetRemote()),
					zap.Error(err))
			} else {
				atomic.AddInt64(&t.stats.MessagesSent, 1)
				logger.Ctx(t.ctx).Debug("消息成功发送到连接",
					zap.String("action", action.String()),
					zap.String("topic", topic),
					zap.String("remote", c.GetRemote()))
			}
		}(conn)
	}

	wg.Wait()

	if errorCount > 0 {
		logger.Ctx(t.ctx).Error("广播消息时出现错误",
			zap.String("action", action.String()),
			zap.String("topic", topic),
			zap.Int64("error_count", errorCount),
			zap.Int("target_connections", len(connections)))
		return fmt.Errorf("failed to broadcast to %d connections", errorCount)
	}

	logger.Ctx(t.ctx).Debug("消息广播完成",
		zap.String("action", action.String()),
		zap.String("topic", topic),
		zap.Int("successful_sends", len(connections)))

	return nil
}

// GetStats 获取统计信息
func (t *GNetTransport) GetStats() *TransportStats {
	stats := *t.stats
	stats.Uptime = time.Since(t.startTime).String()
	stats.CurrentConnections = atomic.LoadInt64(&t.stats.CurrentConnections)
	return &stats
}

// GetConnectionCount 获取连接数
func (t *GNetTransport) GetConnectionCount() int {
	return int(atomic.LoadInt64(&t.stats.CurrentConnections))
}

// GetAllConnections 获取所有连接
func (t *GNetTransport) GetAllConnections() []Connection {
	t.connectionsMu.RLock()
	defer t.connectionsMu.RUnlock()

	connections := make([]Connection, 0, len(t.connections))
	for _, conn := range t.connections {
		connections = append(connections, Connection(conn))
	}
	return connections
}

// SetRouter 设置路由器
func (t *GNetTransport) SetRouter(router Router) {
	t.router = router
	logger.Ctx(t.ctx).Info("Router updated for transport",
		zap.Int("handler_count", router.GetHandlerCount()))
}
