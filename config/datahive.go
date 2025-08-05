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
	DefaultAppName  = "github.com/riven-blade/datahive/-Spider"
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

	// 交易所配置
	Exchanges map[string]*ExchangeConfig `yaml:"exchanges" json:"exchanges"`

	// 缓存配置
	QuestDB *QuestDBConfig `yaml:"questdb" json:"questdb"` // QuestDB配置
	Redis   *RedisConfig   `yaml:"redis" json:"redis"`     // Redis配置
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Host string `yaml:"host" json:"host"` // 监听地址
	Port int    `yaml:"port" json:"port"` // 监听端口
}

// ExchangeConfig 交易所配置
type ExchangeConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`                 // 是否启用
	APIKey          string `yaml:"apiKey" json:"apiKey"`                   // API密钥
	Secret          string `yaml:"secret" json:"secret"`                   // API密钥秘密
	TestNet         bool   `yaml:"testnet" json:"testnet"`                 // 是否使用测试网
	RateLimit       int    `yaml:"rateLimit" json:"rateLimit"`             // 限流速率
	EnableRateLimit bool   `yaml:"enableRateLimit" json:"enableRateLimit"` // 是否启用速率限制
	EnableWebSocket bool   `yaml:"enableWebSocket" json:"enableWebSocket"` // 是否启用WebSocket
	WSMaxReconnect  int    `yaml:"wsMaxReconnect" json:"wsMaxReconnect"`   // WebSocket最大重连次数
	DefaultType     string `yaml:"defaultType" json:"defaultType"`         // 默认市场类型
	Timeout         int    `yaml:"timeout" json:"timeout"`                 // 超时时间(毫秒)
}

func DefaultConfig() *Config {
	return &Config{
		Name:     DefaultAppName,
		LogLevel: DefaultLogLevel,

		Server: &ServerConfig{
			Host: DefaultServerHost,
			Port: DefaultServerPort,
		},

		// 交易所配置
		Exchanges: map[string]*ExchangeConfig{
			"binance": {
				Enabled:         true,
				EnableRateLimit: true, // 启用令牌桶速率限制算法
				EnableWebSocket: true, // 启用WebSocket以支持所有数据类型的实时订阅
				WSMaxReconnect:  5,
				Timeout:         30000,
				RateLimit:       1200,
			},
		},

		// 缓存配置
		QuestDB: NewQuestDBConfig(),
		Redis:   NewRedisConfig(),
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
