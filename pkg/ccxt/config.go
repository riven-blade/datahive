package ccxt

import (
	"time"
)

// ========== 通用交易所配置 ==========

// BaseConfig 通用交易所配置
type BaseConfig struct {
	// API 认证
	APIKey   string `json:"apiKey,omitempty"`
	Secret   string `json:"secret,omitempty"`
	Password string `json:"password,omitempty"` // 部分交易所需要
	UID      string `json:"uid,omitempty"`      // 部分交易所需要

	// 环境配置
	Sandbox bool `json:"sandbox"` // 是否使用沙盒环境
	TestNet bool `json:"testnet"` // 是否使用测试网

	// 网络配置
	Timeout         int    `json:"timeout"`         // 超时时间(毫秒)
	EnableRateLimit bool   `json:"enableRateLimit"` // 是否启用限流
	RateLimit       int    `json:"rateLimit"`       // 限流速率(请求/分钟)
	Proxy           string `json:"proxy,omitempty"` // 代理地址

	// 高级配置
	UserAgent string                 `json:"userAgent"` // 用户代理
	Headers   map[string]string      `json:"headers"`   // 自定义头部
	Options   map[string]interface{} `json:"options"`   // 其他选项

	// 市场类型配置
	DefaultType string `json:"defaultType"` // 默认市场类型: spot, margin, future, swap等

	// WebSocket 配置
	EnableWebSocket bool `json:"enableWebSocket"` // 是否启用WebSocket
	WSMaxReconnect  int  `json:"wsMaxReconnect"`  // WebSocket最大重连次数

	// 交易所特有配置
	RecvWindow int64 `json:"recvWindow"` // 接收窗口时间(毫秒), 主要用于Binance系
}

// DefaultBaseConfig 返回默认基础配置
func DefaultBaseConfig() *BaseConfig {
	return &BaseConfig{
		Sandbox:         false,
		TestNet:         false,
		Timeout:         30000, // 30秒
		EnableRateLimit: true,
		RateLimit:       1200, // 1200请求/分钟
		UserAgent:       "ccxt-go/1.0",
		Headers:         make(map[string]string),
		Options:         make(map[string]interface{}),
		DefaultType:     "spot",
		EnableWebSocket: false,
		WSMaxReconnect:  5,
		RecvWindow:      5000, // 5秒
	}
}

// ========== 实现ExchangeConfig接口 ==========

func (c *BaseConfig) GetAPIKey() string                  { return c.APIKey }
func (c *BaseConfig) GetSecret() string                  { return c.Secret }
func (c *BaseConfig) GetPassword() string                { return c.Password }
func (c *BaseConfig) GetUID() string                     { return c.UID }
func (c *BaseConfig) GetSandbox() bool                   { return c.Sandbox }
func (c *BaseConfig) GetTestnet() bool                   { return c.TestNet }
func (c *BaseConfig) GetTimeout() time.Duration          { return time.Duration(c.Timeout) * time.Millisecond }
func (c *BaseConfig) GetRateLimit() int                  { return c.RateLimit }
func (c *BaseConfig) GetEnableRateLimit() bool           { return c.EnableRateLimit }
func (c *BaseConfig) GetProxy() string                   { return c.Proxy }
func (c *BaseConfig) GetUserAgent() string               { return c.UserAgent }
func (c *BaseConfig) GetMarketType() string              { return c.DefaultType }
func (c *BaseConfig) GetHeaders() map[string]string      { return c.Headers }
func (c *BaseConfig) GetOptions() map[string]interface{} { return c.Options }

func (c *BaseConfig) SetAPIKey(key string)                      { c.APIKey = key }
func (c *BaseConfig) SetSecret(secret string)                   { c.Secret = secret }
func (c *BaseConfig) SetPassword(password string)               { c.Password = password }
func (c *BaseConfig) SetUID(uid string)                         { c.UID = uid }
func (c *BaseConfig) SetSandbox(sandbox bool)                   { c.Sandbox = sandbox }
func (c *BaseConfig) SetTestnet(testnet bool)                   { c.TestNet = testnet }
func (c *BaseConfig) SetTimeout(timeout time.Duration)          { c.Timeout = int(timeout / time.Millisecond) }
func (c *BaseConfig) SetRateLimit(limit int)                    { c.RateLimit = limit }
func (c *BaseConfig) SetEnableRateLimit(enable bool)            { c.EnableRateLimit = enable }
func (c *BaseConfig) SetProxy(proxy string)                     { c.Proxy = proxy }
func (c *BaseConfig) SetUserAgent(userAgent string)             { c.UserAgent = userAgent }
func (c *BaseConfig) SetHeaders(headers map[string]string)      { c.Headers = headers }
func (c *BaseConfig) SetOptions(options map[string]interface{}) { c.Options = options }

// ========== 便捷配置方法 ==========

// WithAuth 设置API认证信息
func (c *BaseConfig) WithAuth(apiKey, secret string) *BaseConfig {
	c.APIKey = apiKey
	c.Secret = secret
	return c
}

// WithWebSocket 启用WebSocket
func (c *BaseConfig) WithWebSocket(enable bool) *BaseConfig {
	c.EnableWebSocket = enable
	return c
}

// WithSandbox 设置沙盒模式
func (c *BaseConfig) WithSandbox(sandbox bool) *BaseConfig {
	c.Sandbox = sandbox
	return c
}

// WithProxy 设置代理
func (c *BaseConfig) WithProxy(proxy string) *BaseConfig {
	c.Proxy = proxy
	return c
}
