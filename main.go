package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"datahive/client"
	"datahive/config"
	"datahive/core"
	"datahive/pkg/logger"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func main() {
	// 创建根context
	ctx := context.Background()

	// 初始化日志 - 使用简单的全局logger
	logger.Ctx(ctx).Info("Logger initialized")

	logger.Ctx(ctx).Info("🚀 Starting DataHive - TCP Server + Exchange Service")

	// 加载配置
	cfg, err := loadConfig(ctx)
	if err != nil {
		logger.Ctx(ctx).Fatal("Failed to load config", zap.Error(err))
	}

	// 创建context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动服务
	dataHive, err := startServices(ctx, cfg)
	if err != nil {
		logger.Ctx(ctx).Fatal("Failed to start services", zap.Error(err))
	}

	// 等待信号
	waitForShutdown(ctx, cancel, dataHive)

	logger.Ctx(ctx).Info("👋 DataHive stopped gracefully")
}

func loadConfig(ctx context.Context) (*config.Config, error) {
	// 尝试从环境变量获取配置文件路径
	configPath := os.Getenv("DATAHIVE_CONFIG")
	if configPath == "" {
		configPath = "config.yaml"
	}

	// 如果配置文件不存在，使用默认配置
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		logger.Ctx(ctx).Info("Config file not found, using default config", zap.String("path", configPath))
		return config.DefaultConfig(), nil
	}

	// 读取配置文件
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 解析YAML配置
	cfg := config.DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	logger.Ctx(ctx).Info("Config loaded successfully", zap.String("path", configPath))

	return cfg, nil
}

func startServices(ctx context.Context, cfg *config.Config) (*core.DataHive, error) {
	// 1. 启动数据中心服务（TCP Server + 交易所业务逻辑 + QuestDB + Redis）
	logger.Ctx(ctx).Info("🚀 Starting DataHive Service...")
	dataHive := core.NewDataHive(cfg)

	if err := dataHive.Start(); err != nil {
		return nil, fmt.Errorf("failed to start datahive service: %w", err)
	}

	// 2. TCP Client可用性验证（便捷调用封装）
	logger.Ctx(ctx).Info("📱 Verifying TCP Client availability...")
	_, err := client.DefaultGNetClient()
	if err != nil {
		logger.Ctx(ctx).Error("Failed to create default TCP client", zap.Error(err))
	} else {
		logger.Ctx(ctx).Info("✅ TCP Client service available")
	}

	logger.Ctx(ctx).Info("✅ All services started successfully")
	logger.Ctx(ctx).Info("🏛️  Architecture: TCP Server (底层) + Exchange Logic (业务层) + Storage (QuestDB + Redis)")
	return dataHive, nil
}

func waitForShutdown(ctx context.Context, cancel context.CancelFunc, dataHive *core.DataHive) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Ctx(ctx).Info("Received shutdown signal", zap.String("signal", sig.String()))

	// 优雅关闭DataHive
	if dataHive != nil {
		logger.Ctx(ctx).Info("Stopping DataHive...")
		if err := dataHive.Stop(); err != nil {
			logger.Ctx(ctx).Error("Failed to stop DataHive gracefully", zap.Error(err))
		}
	}

	// 给服务一些时间完成清理
	go func() {
		time.Sleep(30 * time.Second)
		logger.Ctx(context.Background()).Error("Force shutdown after timeout")
		os.Exit(1)
	}()

	cancel()
}
