package client

import (
	"time"

	"datahive/pkg/logger"
)

// Config TCP客户端配置
type Config struct {
	// 连接配置
	ConnectTimeout       time.Duration
	RequestTimeout       time.Duration
	ReconnectInterval    time.Duration
	MaxReconnectAttempts int

	// 性能配置
	ChannelBufferSize int
	MaxMessageSize    int

	// 功能开关
	EnableAutoReconnect bool
	EnableMetrics       bool

	// 日志配置
	Logger *logger.MLogger
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		ConnectTimeout:       30 * time.Second,
		RequestTimeout:       30 * time.Second,
		ReconnectInterval:    5 * time.Second,
		MaxReconnectAttempts: 5,
		ChannelBufferSize:    100,
		MaxMessageSize:       10 * 1024 * 1024, // 10MB
		EnableAutoReconnect:  true,
		EnableMetrics:        true,
		Logger:               &logger.MLogger{Logger: logger.L()},
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 30 * time.Second
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.ChannelBufferSize <= 0 {
		c.ChannelBufferSize = 100
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = 10 * 1024 * 1024
	}
	if c.Logger == nil {
		c.Logger = &logger.MLogger{Logger: logger.L()}
	}
	return nil
}
