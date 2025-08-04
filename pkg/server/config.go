package server

import (
	"fmt"
	"time"
)

// Config 服务器配置
type Config struct {
	// 网络配置
	Network        string `json:"network"`         // 网络类型：tcp, tcp4, tcp6, unix, unixpacket
	Address        string `json:"address"`         // 监听地址
	MaxConnections int    `json:"max_connections"` // 最大连接数

	// TCP配置
	TCPKeepAlive time.Duration `json:"tcp_keep_alive"` // TCP Keep-Alive间隔
	TCPNoDelay   bool          `json:"tcp_no_delay"`   // TCP NoDelay选项

	// 超时配置
	ReadTimeout  time.Duration `json:"read_timeout"`  // 读取超时
	WriteTimeout time.Duration `json:"write_timeout"` // 写入超时
	IdleTimeout  time.Duration `json:"idle_timeout"`  // 空闲超时

	// 缓冲区配置
	ReadBufferSize  int `json:"read_buffer_size"`  // 读缓冲区大小
	WriteBufferSize int `json:"write_buffer_size"` // 写缓冲区大小

	// GNet配置
	GNet GNetOptions `json:"gnet"` // GNet特定配置

	// 其他配置
	EnableLogging bool   `json:"enable_logging"` // 启用日志
	LogLevel      string `json:"log_level"`      // 日志级别
}

// GNetOptions GNet选项配置
type GNetOptions struct {
	Multicore        bool `json:"multicore"`          // 多核模式
	ReusePort        bool `json:"reuse_port"`         // 端口复用
	LoadBalancing    int  `json:"load_balancing"`     // 负载均衡策略
	NumEventLoop     int  `json:"num_event_loop"`     // 事件循环数量
	ReadBufferCap    int  `json:"read_buffer_cap"`    // 读缓冲区容量
	WriteBufferCap   int  `json:"write_buffer_cap"`   // 写缓冲区容量
	SocketRecvBuffer int  `json:"socket_recv_buffer"` // Socket接收缓冲区
	SocketSendBuffer int  `json:"socket_send_buffer"` // Socket发送缓冲区
}

// GNetConfig gnet特定配置
type GNetConfig struct {
	Config
	Multicore bool       `json:"multicore"`  // 多核模式
	ReusePort bool       `json:"reuse_port"` // 端口复用
	TLSConfig *TLSConfig `json:"tls_config"` // TLS配置（可选）
}

// TLSConfig TLS配置
type TLSConfig struct {
	CertFile string `json:"cert_file"` // 证书文件路径
	KeyFile  string `json:"key_file"`  // 私钥文件路径
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Network:         "tcp",
		Address:         ":7890",
		MaxConnections:  10000,
		TCPKeepAlive:    15 * time.Minute,
		TCPNoDelay:      true,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     120 * time.Second,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		GNet: GNetOptions{
			Multicore:        true,
			ReusePort:        true,
			LoadBalancing:    1, // Round-Robin
			NumEventLoop:     0, // 使用CPU核心数
			ReadBufferCap:    65536,
			WriteBufferCap:   65536,
			SocketRecvBuffer: 0, // 使用系统默认值
			SocketSendBuffer: 0, // 使用系统默认值
		},
		EnableLogging: true,
		LogLevel:      "info",
	}
}

// DefaultGNetConfig 返回默认gnet配置
func DefaultGNetConfig() *GNetConfig {
	return &GNetConfig{
		Config:    *DefaultConfig(),
		Multicore: true,
		ReusePort: true,
		TLSConfig: nil,
	}
}

// SetDefaults 设置默认值
func (c *Config) SetDefaults() {
	if c.Network == "" {
		c.Network = "tcp"
	}
	if c.Address == "" {
		c.Address = ":7890"
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 10000
	}
	if c.TCPKeepAlive <= 0 {
		c.TCPKeepAlive = 15 * time.Minute
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 120 * time.Second
	}
	if c.ReadBufferSize <= 0 {
		c.ReadBufferSize = 4096
	}
	if c.WriteBufferSize <= 0 {
		c.WriteBufferSize = 4096
	}

	// 设置GNet默认值
	if c.GNet.ReadBufferCap <= 0 {
		c.GNet.ReadBufferCap = 65536
	}
	if c.GNet.WriteBufferCap <= 0 {
		c.GNet.WriteBufferCap = 65536
	}
	if c.GNet.LoadBalancing == 0 {
		c.GNet.LoadBalancing = 1 // Round-Robin
	}

	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("address cannot be empty")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("max_connections must be positive")
	}
	if c.ReadTimeout < 0 {
		return fmt.Errorf("read_timeout cannot be negative")
	}
	if c.WriteTimeout < 0 {
		return fmt.Errorf("write_timeout cannot be negative")
	}
	if c.IdleTimeout < 0 {
		return fmt.Errorf("idle_timeout cannot be negative")
	}
	return nil
}

// GetAddr 获取完整地址
func (c *Config) GetAddr() string {
	return c.Address
}

// GetNetwork 获取网络类型
func (c *Config) GetNetwork() string {
	return c.Network
}
