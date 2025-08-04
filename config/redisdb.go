package config

import "time"

// RedisConfig Redis连接配置
type RedisConfig struct {
	Host              string        `yaml:"host" json:"host"`                             // 主机地址
	Port              int           `yaml:"port" json:"port"`                             // 端口
	Password          string        `yaml:"password" json:"password"`                     // 密码 (可选)
	Database          int           `yaml:"database" json:"database"`                     // 数据库编号 (0-15)
	MaxRetries        int           `yaml:"max_retries" json:"max_retries"`               // 最大重试次数
	PoolSize          int           `yaml:"pool_size" json:"pool_size"`                   // 连接池大小
	ConnectionTimeout time.Duration `yaml:"connection_timeout" json:"connection_timeout"` // 连接超时
	QueryTimeout      time.Duration `yaml:"query_timeout" json:"query_timeout"`           // 查询超时
	MaxConnections    int           `yaml:"max_connections" json:"max_connections"`       // 最大连接数
}

// NewRedisConfig 创建默认Redis配置
func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		Host:              "localhost",
		Port:              6379,
		Password:          "",
		Database:          0,
		MaxRetries:        3,
		PoolSize:          10,
		ConnectionTimeout: 10 * time.Second,
		QueryTimeout:      30 * time.Second,
		MaxConnections:    50,
	}
}
