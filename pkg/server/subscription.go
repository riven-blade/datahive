package server

import (
	"context"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/logger"

	"go.uber.org/zap"
)

// SubscriptionStats 订阅统计信息（不包含mutex）
type SubscriptionStats struct {
	ActiveSubscriptions int `json:"active_subscriptions"`
	TotalSubscriptions  int `json:"total_subscriptions"`
	TotalConnections    int `json:"total_connections"`
}

// SubscriptionInfo 订阅信息
type SubscriptionInfo struct {
	ConnectionID string          `json:"connection_id"`
	Topics       map[string]bool `json:"topics"`
	CreatedAt    time.Time       `json:"created_at"`
	LastActivity time.Time       `json:"last_activity"`
}

// Subscription 订阅服务接口
type Subscription interface {
	Subscribe(connID, topic string) error
	Unsubscribe(connID, topic string) error
	GetSubscribers(topic string) []string
	GetSubscriptions(connID string) []string
	RemoveConnection(connID string)
	GetStats() *SubscriptionStats
}

// StandardSubscription 标准订阅服务
type StandardSubscription struct {
	// 订阅映射: connID -> topics
	connections map[string]*SubscriptionInfo
	connMu      sync.RWMutex

	// 反向映射: topic -> []connID
	topics  map[string][]string
	topicMu sync.RWMutex

	// 统计信息和它的互斥锁
	stats   *SubscriptionStats
	statsMu sync.RWMutex
}

// NewSubscription 创建订阅服务
func NewSubscription() Subscription {
	return &StandardSubscription{
		connections: make(map[string]*SubscriptionInfo),
		topics:      make(map[string][]string),
		stats:       &SubscriptionStats{},
	}
}

// Subscribe 添加订阅
func (s *StandardSubscription) Subscribe(connID, topic string) error {
	ctx := context.Background()

	// 处理连接信息
	s.connMu.Lock()
	connInfo, exists := s.connections[connID]
	if !exists {
		connInfo = &SubscriptionInfo{
			ConnectionID: connID,
			Topics:       make(map[string]bool),
			CreatedAt:    time.Now(),
			LastActivity: time.Now(),
		}
		s.connections[connID] = connInfo
	}

	// 如果已经订阅，直接返回
	if connInfo.Topics[topic] {
		s.connMu.Unlock()
		return nil
	}

	connInfo.Topics[topic] = true
	connInfo.LastActivity = time.Now()
	s.connMu.Unlock()

	// 处理主题映射
	s.topicMu.Lock()
	subscribers, exists := s.topics[topic]
	if !exists {
		s.topics[topic] = []string{connID}
	} else {
		// 检查是否已存在
		found := false
		for _, subscriber := range subscribers {
			if subscriber == connID {
				found = true
				break
			}
		}
		if !found {
			s.topics[topic] = append(subscribers, connID)
		}
	}
	s.topicMu.Unlock()

	// 更新统计
	s.statsMu.Lock()
	s.stats.ActiveSubscriptions++
	s.stats.TotalSubscriptions++
	s.statsMu.Unlock()

	logger.Ctx(ctx).Debug("Subscription added",
		zap.String("connection_id", connID),
		zap.String("topic", topic))

	return nil
}

// Unsubscribe 取消订阅
func (s *StandardSubscription) Unsubscribe(connID, topic string) error {
	ctx := context.Background()

	// 处理连接信息
	s.connMu.Lock()
	connInfo, exists := s.connections[connID]
	if !exists || !connInfo.Topics[topic] {
		s.connMu.Unlock()
		return nil // 没有订阅，直接返回
	}

	delete(connInfo.Topics, topic)
	connInfo.LastActivity = time.Now()
	s.connMu.Unlock()

	// 处理主题映射
	s.topicMu.Lock()
	subscribers, exists := s.topics[topic]
	if exists {
		// 移除连接ID
		for i, subscriber := range subscribers {
			if subscriber == connID {
				s.topics[topic] = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
		// 如果没有订阅者了，删除主题
		if len(s.topics[topic]) == 0 {
			delete(s.topics, topic)
		}
	}
	s.topicMu.Unlock()

	// 更新统计
	s.statsMu.Lock()
	s.stats.ActiveSubscriptions--
	s.statsMu.Unlock()

	logger.Ctx(ctx).Debug("Subscription removed",
		zap.String("connection_id", connID),
		zap.String("topic", topic))

	return nil
}

// GetSubscribers 获取主题的所有订阅者
func (s *StandardSubscription) GetSubscribers(topic string) []string {
	s.topicMu.RLock()
	defer s.topicMu.RUnlock()

	subscribers, exists := s.topics[topic]
	if !exists {
		return []string{}
	}

	// 返回副本，避免外部修改
	result := make([]string, len(subscribers))
	copy(result, subscribers)
	return result
}

// GetSubscriptions 获取连接的所有订阅
func (s *StandardSubscription) GetSubscriptions(connID string) []string {
	s.connMu.RLock()
	defer s.connMu.RUnlock()

	connInfo, exists := s.connections[connID]
	if !exists {
		return []string{}
	}

	// 返回主题列表
	topics := make([]string, 0, len(connInfo.Topics))
	for topic := range connInfo.Topics {
		topics = append(topics, topic)
	}
	return topics
}

// RemoveConnection 移除连接及其所有订阅
func (s *StandardSubscription) RemoveConnection(connID string) {
	ctx := context.Background()

	// 获取连接的所有订阅主题
	s.connMu.RLock()
	connInfo, exists := s.connections[connID]
	if !exists {
		s.connMu.RUnlock()
		return
	}

	topics := make([]string, 0, len(connInfo.Topics))
	for topic := range connInfo.Topics {
		topics = append(topics, topic)
	}
	s.connMu.RUnlock()

	// 取消所有订阅
	removedCount := 0
	for _, topic := range topics {
		if s.Unsubscribe(connID, topic) == nil {
			removedCount++
		}
	}

	// 移除连接信息
	s.connMu.Lock()
	delete(s.connections, connID)
	s.connMu.Unlock()

	// 更新统计
	s.statsMu.Lock()
	s.stats.TotalConnections--
	s.statsMu.Unlock()

	logger.Ctx(ctx).Debug("Connection removed",
		zap.String("connection_id", connID),
		zap.Int("removed_subscriptions", removedCount))
}

// GetStats 获取订阅统计信息
func (s *StandardSubscription) GetStats() *SubscriptionStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()

	// 手动复制数据字段，不复制mutex
	return &SubscriptionStats{
		ActiveSubscriptions: s.stats.ActiveSubscriptions,
		TotalSubscriptions:  s.stats.TotalSubscriptions,
		TotalConnections:    s.stats.TotalConnections,
	}
}
