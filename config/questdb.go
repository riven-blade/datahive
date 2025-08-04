package config

import "time"

// QuestDBConfig QuestDB 配置
type QuestDBConfig struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Database string `json:"database" yaml:"database"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`

	// 性能配置
	MaxConnections    int           `json:"maxConnections" yaml:"maxConnections"`
	ConnectionTimeout time.Duration `json:"connectionTimeout" yaml:"connectionTimeout"`
	QueryTimeout      time.Duration `json:"queryTimeout" yaml:"queryTimeout"`

	// 批量写入配置
	BatchSize     int           `json:"batchSize" yaml:"batchSize"`
	FlushInterval time.Duration `json:"flushInterval" yaml:"flushInterval"`

	// 健康检查配置
	HealthCheckInterval time.Duration `json:"healthCheckInterval" yaml:"healthCheckInterval"`
	EnableAutoReconnect bool          `json:"enableAutoReconnect" yaml:"enableAutoReconnect"`
}

// NewQuestDBConfig 创建默认配置
func NewQuestDBConfig() *QuestDBConfig {
	return &QuestDBConfig{
		Host:                "localhost",
		Port:                8812,
		Database:            "qdb",
		Username:            "admin",
		Password:            "quest",
		MaxConnections:      10,
		ConnectionTimeout:   10 * time.Second, // 缩短连接超时，立即响应
		QueryTimeout:        15 * time.Second, // 缩短查询超时
		BatchSize:           100,              // 减小批次大小，更快响应
		FlushInterval:       1 * time.Second,  // 更频繁的刷新，立即写入
		HealthCheckInterval: 30 * time.Second, // 健康检查间隔
		EnableAutoReconnect: true,             // 启用自动重连
	}
}
