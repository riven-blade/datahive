package bybit

import (
	"fmt"
)

// ========== Bybit 配置 ==========

// Config Bybit 交易所配置
type Config struct {
	// API 认证
	APIKey string `json:"apiKey,omitempty"`
	Secret string `json:"secret,omitempty"`

	// 环境配置
	Sandbox bool `json:"sandbox"` // 是否使用沙盒环境
	TestNet bool `json:"testnet"` // 是否使用测试网

	// 网络配置
	Timeout         int    `json:"timeout"`         // 超时时间(毫秒)
	EnableRateLimit bool   `json:"enableRateLimit"` // 是否启用限流
	Proxy           string `json:"proxy,omitempty"` // 代理地址

	// 高级配置
	RecvWindow int64                  `json:"recvWindow"` // 接收窗口时间(毫秒)
	UserAgent  string                 `json:"userAgent"`  // 用户代理
	Headers    map[string]string      `json:"headers"`    // 自定义头部
	Options    map[string]interface{} `json:"options"`    // 其他选项

	// 市场类型配置
	DefaultType string `json:"defaultType"` // 默认市场类型: spot, linear, inverse, option

	// WebSocket 配置
	EnableWebSocket bool `json:"enableWebSocket"` // 是否启用WebSocket
	WSMaxReconnect  int  `json:"wsMaxReconnect"`  // WebSocket最大重连次数

	// Bybit特有配置
	Category string `json:"category"` // 产品类型: spot, linear, inverse, option
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Sandbox:         false,
		TestNet:         false,
		Timeout:         30000, // 30秒
		EnableRateLimit: true,
		RecvWindow:      5000, // 5秒
		UserAgent:       "ccxt-go-bybit/1.0",
		Headers:         make(map[string]string),
		Options:         make(map[string]interface{}),
		DefaultType:     "linear", // Bybit主要是期货交易所
		EnableWebSocket: false,
		WSMaxReconnect:  3,
		Category:        "linear",
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Timeout < 0 {
		return fmt.Errorf("timeout cannot be negative")
	}

	if c.RecvWindow < 0 {
		return fmt.Errorf("recvWindow cannot be negative")
	}

	if c.RecvWindow > 60000 {
		return fmt.Errorf("recvWindow cannot exceed 60000ms")
	}

	// 验证市场类型
	validTypes := map[string]bool{
		"spot":    true,
		"linear":  true,
		"inverse": true,
		"option":  true,
	}

	if !validTypes[c.DefaultType] {
		return fmt.Errorf("invalid defaultType: %s", c.DefaultType)
	}

	// 验证Category
	validCategories := map[string]bool{
		"spot":    true,
		"linear":  true,
		"inverse": true,
		"option":  true,
	}

	if !validCategories[c.Category] {
		return fmt.Errorf("invalid category: %s", c.Category)
	}

	return nil
}

// Clone 克隆配置
func (c *Config) Clone() *Config {
	clone := *c

	// 深拷贝 map
	clone.Headers = make(map[string]string)
	for k, v := range c.Headers {
		clone.Headers[k] = v
	}

	clone.Options = make(map[string]interface{})
	for k, v := range c.Options {
		clone.Options[k] = v
	}

	return &clone
}

// ========== 配置构建器 ==========

// ConfigBuilder 配置构建器
type ConfigBuilder struct {
	config *Config
}

// NewConfigBuilder 创建配置构建器
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{
		config: DefaultConfig(),
	}
}

// WithCredentials 设置API凭证
func (b *ConfigBuilder) WithCredentials(apiKey, secret string) *ConfigBuilder {
	b.config.APIKey = apiKey
	b.config.Secret = secret
	return b
}

// WithTestNet 设置是否使用测试网
func (b *ConfigBuilder) WithTestNet(testnet bool) *ConfigBuilder {
	b.config.TestNet = testnet
	return b
}

// WithSandbox 设置是否使用沙盒
func (b *ConfigBuilder) WithSandbox(sandbox bool) *ConfigBuilder {
	b.config.Sandbox = sandbox
	return b
}

// WithTimeout 设置超时时间
func (b *ConfigBuilder) WithTimeout(timeout int) *ConfigBuilder {
	b.config.Timeout = timeout
	return b
}

// WithRateLimit 设置是否启用限流
func (b *ConfigBuilder) WithRateLimit(enable bool) *ConfigBuilder {
	b.config.EnableRateLimit = enable
	return b
}

// WithProxy 设置代理
func (b *ConfigBuilder) WithProxy(proxy string) *ConfigBuilder {
	b.config.Proxy = proxy
	return b
}

// WithRecvWindow 设置接收窗口
func (b *ConfigBuilder) WithRecvWindow(recvWindow int64) *ConfigBuilder {
	b.config.RecvWindow = recvWindow
	return b
}

// WithUserAgent 设置用户代理
func (b *ConfigBuilder) WithUserAgent(userAgent string) *ConfigBuilder {
	b.config.UserAgent = userAgent
	return b
}

// WithDefaultType 设置默认市场类型
func (b *ConfigBuilder) WithDefaultType(marketType string) *ConfigBuilder {
	b.config.DefaultType = marketType
	return b
}

// WithCategory 设置产品类型
func (b *ConfigBuilder) WithCategory(category string) *ConfigBuilder {
	b.config.Category = category
	return b
}

// WithWebSocket 设置WebSocket
func (b *ConfigBuilder) WithWebSocket(enable bool, maxReconnect int) *ConfigBuilder {
	b.config.EnableWebSocket = enable
	b.config.WSMaxReconnect = maxReconnect
	return b
}

// WithHeader 添加头部
func (b *ConfigBuilder) WithHeader(key, value string) *ConfigBuilder {
	if b.config.Headers == nil {
		b.config.Headers = make(map[string]string)
	}
	b.config.Headers[key] = value
	return b
}

// WithOption 添加选项
func (b *ConfigBuilder) WithOption(key string, value interface{}) *ConfigBuilder {
	if b.config.Options == nil {
		b.config.Options = make(map[string]interface{})
	}
	b.config.Options[key] = value
	return b
}

// Build 构建配置
func (b *ConfigBuilder) Build() (*Config, error) {
	if err := b.config.Validate(); err != nil {
		return nil, err
	}
	return b.config.Clone(), nil
}

// MustBuild 构建配置(遇到错误会panic)
func (b *ConfigBuilder) MustBuild() *Config {
	config, err := b.Build()
	if err != nil {
		panic(err)
	}
	return config
}

// ========== 预设配置 ==========

// ProductionConfig 生产环境配置
func ProductionConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithTestNet(false).
		WithSandbox(false).
		WithTimeout(30000).
		WithRateLimit(true).
		WithRecvWindow(5000).
		MustBuild()
}

// TestNetConfig 测试网配置
func TestNetConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithTestNet(true).
		WithSandbox(false).
		WithTimeout(30000).
		WithRateLimit(true).
		WithRecvWindow(5000).
		MustBuild()
}

// SandboxConfig 沙盒配置
func SandboxConfig() *Config {
	return NewConfigBuilder().
		WithSandbox(true).
		WithTimeout(30000).
		WithRateLimit(false). // 沙盒环境通常不需要限流
		WithRecvWindow(5000).
		MustBuild()
}

// ReadOnlyConfig 只读配置(无需API密钥)
func ReadOnlyConfig() *Config {
	return NewConfigBuilder().
		WithTimeout(30000).
		WithRateLimit(true).
		MustBuild()
}

// LinearConfig 线性合约配置
func LinearConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithDefaultType("linear").
		WithCategory("linear").
		WithTimeout(30000).
		WithRateLimit(true).
		MustBuild()
}

// InverseConfig 反向合约配置
func InverseConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithDefaultType("inverse").
		WithCategory("inverse").
		WithTimeout(30000).
		WithRateLimit(true).
		MustBuild()
}

// SpotConfig 现货配置
func SpotConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithDefaultType("spot").
		WithCategory("spot").
		WithTimeout(30000).
		WithRateLimit(true).
		MustBuild()
}

// OptionConfig 期权配置
func OptionConfig(apiKey, secret string) *Config {
	return NewConfigBuilder().
		WithCredentials(apiKey, secret).
		WithDefaultType("option").
		WithCategory("option").
		WithTimeout(30000).
		WithRateLimit(true).
		MustBuild()
}

// ========== 配置验证辅助函数 ==========

// IsValidCredentials 检查是否有有效的API凭证
func (c *Config) IsValidCredentials() bool {
	return c.APIKey != "" && c.Secret != ""
}

// RequiresAuth 检查是否需要认证
func (c *Config) RequiresAuth() bool {
	return !c.Sandbox && c.IsValidCredentials()
}

// GetBaseURL 获取基础URL
func (c *Config) GetBaseURL() string {
	if c.TestNet {
		return "https://api-testnet.bybit.com"
	}
	if c.Sandbox {
		return "https://api-testnet.bybit.com"
	}
	return "https://api.bybit.com"
}

// GetWebSocketURL 获取WebSocket URL
func (c *Config) GetWebSocketURL() string {
	if c.TestNet {
		switch c.Category {
		case "linear":
			return "wss://stream-testnet.bybit.com/v5/public/linear"
		case "inverse":
			return "wss://stream-testnet.bybit.com/v5/public/inverse"
		case "spot":
			return "wss://stream-testnet.bybit.com/v5/public/spot"
		case "option":
			return "wss://stream-testnet.bybit.com/v5/public/option"
		default:
			return "wss://stream-testnet.bybit.com/v5/public/linear"
		}
	}

	switch c.Category {
	case "linear":
		return "wss://stream.bybit.com/v5/public/linear"
	case "inverse":
		return "wss://stream.bybit.com/v5/public/inverse"
	case "spot":
		return "wss://stream.bybit.com/v5/public/spot"
	case "option":
		return "wss://stream.bybit.com/v5/public/option"
	default:
		return "wss://stream.bybit.com/v5/public/linear"
	}
}

// GetPrivateWebSocketURL 获取私有WebSocket URL
func (c *Config) GetPrivateWebSocketURL() string {
	if c.TestNet {
		return "wss://stream-testnet.bybit.com/v5/private"
	}
	return "wss://stream.bybit.com/v5/private"
}
