package config

import (
	"fmt"
	"time"
)

// =============================================================================
// 常量定义
// =============================================================================

const (
	// 应用配置
	DefaultAppName  = "DataHive-Spider"
	DefaultLogLevel = "info"

	// 服务器配置
	DefaultServerHost = "0.0.0.0"
	DefaultServerPort = 6789

	// 缓存配置
	DefaultRealtimeTTL = 5 // 分钟

	// WebSocket 配置
	DefaultKlineChannelBuffer = 200
	DefaultTradeChannelBuffer = 500
	DefaultDepthChannelBuffer = 300
	DefaultPriceChannelBuffer = 100

	// 监控配置
	DefaultMetricsCollectionInterval = 10 * time.Second
	DefaultHighErrorRateThreshold    = 0.10 // 10%
	DefaultMediumErrorRateThreshold  = 0.05 // 5%
	DefaultChannelOverflowThreshold  = 10
	DefaultMessageTimeoutThreshold   = 60 * time.Second
	DefaultHighLatencyThreshold      = 1 * time.Second

	// 告警冷却时间
	DefaultErrorRateAlertCooldown  = 5 * time.Minute
	DefaultChannelOverflowCooldown = 2 * time.Minute
	DefaultConnectionAlertCooldown = 1 * time.Minute
	DefaultMessageTimeoutCooldown  = 3 * time.Minute
	DefaultLatencyAlertCooldown    = 5 * time.Minute
)

// =============================================================================
// 核心配置
// =============================================================================
type Config struct {
	// 基础配置
	Name     string `yaml:"name" json:"name"`           // 服务名称
	LogLevel string `yaml:"log_level" json:"log_level"` // 日志级别

	// 服务器配置
	Server *ServerConfig `yaml:"server" json:"server"`

	// 缓存配置
	Cache *CacheConfig `yaml:"cache" json:"cache"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Host string `yaml:"host" json:"host"` // 监听地址
	Port int    `yaml:"port" json:"port"` // 监听端口
}

// CacheConfig 缓存配置
type CacheConfig struct {
	QuestDB *QuestDBConfig `yaml:"questdb" json:"questdb"` // QuestDB配置
	Redis   *RedisConfig   `yaml:"redis" json:"redis"`     // Redis配置
}

// =============================================================================
// 默认配置和构造函数
// =============================================================================

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Name:     DefaultAppName,
		LogLevel: DefaultLogLevel,

		Server: &ServerConfig{
			Host: DefaultServerHost,
			Port: DefaultServerPort,
		},

		Cache: &CacheConfig{
			QuestDB: NewQuestDBConfig(),
			Redis:   NewRedisConfig(),
		},
	}
}

// =============================================================================
// 配置验证和工具方法
// =============================================================================

// GetAddress 返回监听地址
func (c *Config) GetAddress() string {
	if c.Server == nil {
		return fmt.Sprintf(":%d", DefaultServerPort)
	}
	if c.Server.Host == "" || c.Server.Host == "0.0.0.0" {
		return fmt.Sprintf(":%d", c.Server.Port)
	}
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// Validate 验证配置是否有效
func (c *Config) Validate() error {
	// 验证服务器配置
	if c.Server == nil {
		c.Server = &ServerConfig{
			Host: DefaultServerHost,
			Port: DefaultServerPort,
		}
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		c.Server.Port = DefaultServerPort
	}

	// 验证缓存配置
	if c.Cache == nil {
		c.Cache = &CacheConfig{
			QuestDB: NewQuestDBConfig(),
			Redis:   NewRedisConfig(),
		}
	}

	return nil
}

// GetChannelBufferSize 获取指定类型的channel缓冲区大小 - 使用常量
func GetChannelBufferSize(dataType string) int {
	switch dataType {
	case "kline":
		return DefaultKlineChannelBuffer
	case "trade":
		return DefaultTradeChannelBuffer
	case "depth":
		return DefaultDepthChannelBuffer
	case "price":
		return DefaultPriceChannelBuffer
	default:
		return 100 // 通用默认值
	}
}

// GetRateLimit 获取指定类型的速率限制 - 使用常量
func GetRateLimit(dataType string) int64 {
	// 使用常量值，不再需要复杂的配置
	switch dataType {
	case "kline":
		return 200
	case "trade":
		return 500
	case "depth":
		return 300
	case "price":
		return 100
	default:
		return 100
	}
}
