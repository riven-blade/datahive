package binance

import (
	"fmt"
)

// ========== Binance 配置 ==========

// Config Binance 交易所配置
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
	MarketType string `json:"marketType"` // 市场类型: spot, margin, futures

	// WebSocket 配置
	EnableWebSocket bool `json:"enableWebSocket"` // 是否启用WebSocket
	WSMaxReconnect  int  `json:"wsMaxReconnect"`  // WebSocket最大重连次数
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Sandbox:         false,
		TestNet:         false,
		Timeout:         30000, // 30秒
		EnableRateLimit: true,
		RecvWindow:      5000, // 5秒
		UserAgent:       "ccxt-go-binance/1.0",
		Headers:         make(map[string]string),
		Options:         make(map[string]interface{}),
		MarketType:      "spot",
		EnableWebSocket: false,
		WSMaxReconnect:  3,
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
		"spot":   true,
		"margin": true,
		"future": true,
		"option": true,
	}

	if !validTypes[c.MarketType] {
		return fmt.Errorf("invalid marketType: %s", c.MarketType)
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

// SetAPICredentials 设置API凭证
func (c *Config) SetAPICredentials(apiKey, secret string) {
	c.APIKey = apiKey
	c.Secret = secret
}

// SetEnvironment 设置环境
func (c *Config) SetEnvironment(testnet, sandbox bool) {
	c.TestNet = testnet
	c.Sandbox = sandbox
}

// SetNetworking 设置网络相关配置
func (c *Config) SetNetworking(timeout int, enableRateLimit bool, proxy string) {
	c.Timeout = timeout
	c.EnableRateLimit = enableRateLimit
	c.Proxy = proxy
}

// SetWebSocket 设置WebSocket配置
func (c *Config) SetWebSocket(enable bool, maxReconnect int) {
	c.EnableWebSocket = enable
	c.WSMaxReconnect = maxReconnect
}

// AddHeader 添加自定义头部
func (c *Config) AddHeader(key, value string) {
	if c.Headers == nil {
		c.Headers = make(map[string]string)
	}
	c.Headers[key] = value
}

// SetOption 设置选项
func (c *Config) SetOption(key string, value interface{}) {
	if c.Options == nil {
		c.Options = make(map[string]interface{})
	}
	c.Options[key] = value
}

// GetOption 获取选项
func (c *Config) GetOption(key string) (interface{}, bool) {
	if c.Options == nil {
		return nil, false
	}
	value, exists := c.Options[key]
	return value, exists
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

// WithMarketType 设置市场类型
func (b *ConfigBuilder) WithMarketType(marketType string) *ConfigBuilder {
	b.config.MarketType = marketType
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
		return "https://testnet.binance.vision"
	}
	if c.Sandbox {
		return "https://testnet.binance.vision"
	}
	return "https://api.binance.com"
}

// GetWebSocketURL 获取WebSocket URL
func (c *Config) GetWebSocketURL() string {
	if c.TestNet {
		return "wss://testnet.binance.vision/ws"
	}
	if c.Sandbox {
		return "wss://testnet.binance.vision/ws"
	}
	return "wss://stream.binance.com:9443/ws"
}

// GetFuturesURL 获取期货URL
func (c *Config) GetFuturesURL() string {
	if c.TestNet {
		return "https://testnet.binancefuture.com"
	}
	return "https://fapi.binance.com"
}

// GetOptionsURL 获取期权URL
func (c *Config) GetOptionsURL() string {
	if c.TestNet {
		return "https://testnet.binanceops.com"
	}
	return "https://eapi.binance.com"
}
