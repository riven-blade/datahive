package storage

import (
	"context"
	"datahive/config"
	"datahive/pkg/logger"
	"sync"

	"go.uber.org/zap"
)

// StorageManager 存储管理器
type StorageManager struct {
	questDB TimeSeriesStorage
	redis   KVStorage
	mu      sync.RWMutex
}

// NewStorageManager 创建存储管理器
func NewStorageManager(ctx context.Context, questDBConf *config.QuestDBConfig, redisConf *config.RedisConfig) (*StorageManager, error) {
	manager := &StorageManager{
		mu: sync.RWMutex{},
	}

	// 初始化 QuestDB
	if questDBConf != nil {
		questDB, err := NewQuestDBStorage(ctx, questDBConf)
		if err != nil {
			return nil, ErrConnectionError("failed to initialize QuestDB", err)
		}
		manager.questDB = questDB
		logger.Ctx(ctx).Info("QuestDB存储初始化成功")
	}

	// 初始化 Redis
	if redisConf != nil {
		redis, err := NewRedisStorage(ctx, redisConf)
		if err != nil {
			if manager.questDB != nil {
				manager.questDB.Close()
			}
			return nil, ErrConnectionError("failed to initialize Redis", err)
		}
		manager.redis = redis
		logger.Ctx(ctx).Info("Redis存储初始化成功")
	}

	return manager, nil
}

// GetTimeSeriesStorage 获取时序存储实例
func (sm *StorageManager) GetTimeSeriesStorage() TimeSeriesStorage {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.questDB
}

// GetKVStorage 获取键值存储实例
func (sm *StorageManager) GetKVStorage() KVStorage {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.redis
}

// IsHealthy 检查所有存储是否健康
func (sm *StorageManager) IsHealthy() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	questDBHealthy := sm.questDB == nil || sm.questDB.IsHealthy()
	redisHealthy := sm.redis == nil || sm.redis.IsHealthy()

	return questDBHealthy && redisHealthy
}

// Close 关闭所有存储连接
func (sm *StorageManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logger.Ctx(context.Background()).Info("开始关闭存储管理器...")

	var err error

	// 关闭 QuestDB
	if sm.questDB != nil {
		if closeErr := sm.questDB.Close(); closeErr != nil {
			logger.Ctx(context.Background()).Error("关闭QuestDB失败", zap.Error(closeErr))
			err = closeErr
		} else {
			logger.Ctx(context.Background()).Info("QuestDB已关闭")
		}
		sm.questDB = nil
	}

	// 关闭 Redis
	if sm.redis != nil {
		if closeErr := sm.redis.Close(); closeErr != nil {
			logger.Ctx(context.Background()).Error("关闭Redis失败", zap.Error(closeErr))
			err = closeErr
		} else {
			logger.Ctx(context.Background()).Info("Redis已关闭")
		}
		sm.redis = nil
	}

	logger.Ctx(context.Background()).Info("存储管理器已关闭")
	return err
}

// GetStorageStats 获取存储统计信息
func (sm *StorageManager) GetStorageStats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := make(map[string]interface{})

	if sm.questDB != nil {
		if questDBStorage, ok := sm.questDB.(*QuestDBStorage); ok {
			stats["questdb"] = questDBStorage.GetDetailedHealth()
		}
	}

	if sm.redis != nil {
		if redisStorage, ok := sm.redis.(*RedisStorage); ok {
			stats["redis"] = redisStorage.GetRedisStats()
		}
	}

	stats["overall_healthy"] = sm.IsHealthy()

	return stats
}

// Ping 检查所有存储连接
func (sm *StorageManager) Ping(ctx context.Context) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.questDB != nil {
		if err := sm.questDB.Ping(ctx); err != nil {
			return ErrConnectionError("QuestDB ping failed", err)
		}
	}

	if sm.redis != nil {
		if err := sm.redis.Ping(ctx); err != nil {
			return ErrConnectionError("Redis ping failed", err)
		}
	}

	return nil
}

// 便捷创建函数

// NewQuestDBOnly 仅创建QuestDB存储
func NewQuestDBOnly(ctx context.Context, conf *config.QuestDBConfig) (*StorageManager, error) {
	return NewStorageManager(ctx, conf, nil)
}

// NewRedisOnly 仅创建Redis存储
func NewRedisOnly(ctx context.Context, conf *config.RedisConfig) (*StorageManager, error) {
	return NewStorageManager(ctx, nil, conf)
}

// NewDefaultStorage 使用默认配置创建存储
func NewDefaultStorage(ctx context.Context) (*StorageManager, error) {
	return NewStorageManager(ctx, config.NewQuestDBConfig(), config.NewRedisConfig())
}
