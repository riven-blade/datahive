package client

import (
	"fmt"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// Config 服务器配置
type Config struct {
	// 网络配置
	ReadTimeout    time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout" yaml:"write_timeout"`
	MaxMessageSize int           `json:"max_message_size" yaml:"max_message_size"`

	// 服务器配置
	ListenAddr     string `json:"listen_addr" yaml:"listen_addr"`         // 监听地址，内部格式: ":port", gnet使用: "tcp://:port"
	MaxConnections int    `json:"max_connections" yaml:"max_connections"` // 最大连接数

	// TCP配置
	TCPKeepAlive time.Duration `json:"tcp_keep_alive" yaml:"tcp_keep_alive"`
	TCPNoDelay   bool          `json:"tcp_no_delay" yaml:"tcp_no_delay"`

	// GNet高性能配置
	GNet GNetConfig `json:"gnet" yaml:"gnet"`
}

// GNetConfig gnet高性能配置
type GNetConfig struct {
	Multicore        bool               `json:"multicore" yaml:"multicore"`
	ReusePort        bool               `json:"reuse_port" yaml:"reuse_port"`
	LoadBalancing    gnet.LoadBalancing `json:"load_balancing" yaml:"load_balancing"`
	NumEventLoop     int                `json:"num_event_loop" yaml:"num_event_loop"`
	ReadBufferCap    int                `json:"read_buffer_cap" yaml:"read_buffer_cap"`
	WriteBufferCap   int                `json:"write_buffer_cap" yaml:"write_buffer_cap"`
	SocketRecvBuffer int                `json:"socket_recv_buffer" yaml:"socket_recv_buffer"`
	SocketSendBuffer int                `json:"socket_send_buffer" yaml:"socket_send_buffer"`
}

// DefaultConfig 返回默认高性能配置
func DefaultConfig() *Config {
	return &Config{
		// 网络配置
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxMessageSize: 16 * 1024 * 1024, // 16MB

		// 服务器配置
		ListenAddr:     ":6789",
		MaxConnections: 10000, // 提高连接数限制

		// TCP配置
		TCPKeepAlive: 30 * time.Second, // 30秒keepalive
		TCPNoDelay:   true,             // 启用nodelay获得更低延迟

		// GNet高性能配置
		GNet: GNetConfig{
			Multicore:        true,            // 多核支持
			ReusePort:        true,            // 端口复用
			LoadBalancing:    gnet.RoundRobin, // 轮询负载均衡
			NumEventLoop:     0,               // 自动检测CPU核心数
			ReadBufferCap:    16384,           // 16KB读缓冲
			WriteBufferCap:   16384,           // 16KB写缓冲
			SocketRecvBuffer: 2 * 1024 * 1024, // 2MB socket接收缓冲
			SocketSendBuffer: 2 * 1024 * 1024, // 2MB socket发送缓冲
		},
	}
}

// SetDefaults 设置默认值
func (c *Config) SetDefaults() {
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = 16 * 1024 * 1024 // 16MB
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":6789"
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 10000
	}
	if c.TCPKeepAlive <= 0 {
		c.TCPKeepAlive = 30 * time.Second
	}

	// GNet配置默认值
	if c.GNet.ReadBufferCap <= 0 {
		c.GNet.ReadBufferCap = 16384
	}
	if c.GNet.WriteBufferCap <= 0 {
		c.GNet.WriteBufferCap = 16384
	}
	if c.GNet.SocketRecvBuffer <= 0 {
		c.GNet.SocketRecvBuffer = 2 * 1024 * 1024
	}
	if c.GNet.SocketSendBuffer <= 0 {
		c.GNet.SocketSendBuffer = 2 * 1024 * 1024
	}
}

// Validate 验证配置参数
func (c *Config) Validate() error {
	if c.ReadTimeout <= 0 {
		return fmt.Errorf("read timeout must be positive")
	}
	if c.WriteTimeout <= 0 {
		return fmt.Errorf("write timeout must be positive")
	}
	if c.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be positive")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be positive")
	}
	if c.TCPKeepAlive <= 0 {
		return fmt.Errorf("tcp keep alive must be positive")
	}

	// 验证GNet配置
	if c.GNet.ReadBufferCap <= 0 {
		return fmt.Errorf("gnet read buffer capacity must be positive")
	}
	if c.GNet.WriteBufferCap <= 0 {
		return fmt.Errorf("gnet write buffer capacity must be positive")
	}

	return nil
}
