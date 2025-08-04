package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// 临时的缓存接口定义 - 替代复杂的DBCache
type SimpleCache interface {
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
}

// UnifiedStorage 统一存储实现
type UnifiedStorage struct {
	cache SimpleCache
}

// NewUnifiedStorage 创建统一存储
func NewUnifiedStorage(cache SimpleCache) *UnifiedStorage {
	return &UnifiedStorage{
		cache: cache,
	}
}

// Save 保存数据点 - 实现Storage接口
func (s *UnifiedStorage) Save(ctx context.Context, data DataPoint) error {
	if s.cache == nil {
		// 如果没有缓存，直接返回成功（模拟保存）
		return nil
	}

	// 简化实现：将数据序列化并缓存
	key := fmt.Sprintf("%s:%s:%s:%s", data.Type, data.Exchange, data.Market, data.Symbol)
	value, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 缓存数据，TTL 1小时
	return s.cache.Set(ctx, key, value, time.Hour)
}

// Query 查询数据 - 实现Storage接口
func (s *UnifiedStorage) Query(ctx context.Context, query Query) ([]DataPoint, error) {
	if s.cache == nil {
		return []DataPoint{}, nil
	}

	// 简化实现：从缓存查询
	key := fmt.Sprintf("%s:%s:%s:%s", query.Type, query.Exchange, query.Market, query.Symbol)
	value, err := s.cache.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	var data DataPoint
	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return nil, err
	}

	return []DataPoint{data}, nil
}

// Cache 缓存数据 - 实现Storage接口
func (s *UnifiedStorage) Cache(ctx context.Context, key string, value any, ttl int64) error {
	if s.cache == nil {
		return nil
	}

	duration := time.Duration(ttl) * time.Second
	return s.cache.Set(ctx, key, value, duration)
}

// Get 获取缓存数据 - 实现Storage接口
func (s *UnifiedStorage) Get(ctx context.Context, key string) (any, error) {
	if s.cache == nil {
		return nil, fmt.Errorf("cache not available")
	}

	return s.cache.Get(ctx, key)
}
