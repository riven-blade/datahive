# 简化的批量订阅实现

## 问题背景

之前的实现有以下复杂性：

1. **Symbol绑定实例**：每个symbol固定分配到一个WebSocket实例
2. **复杂的批量管理器**：包含多个配置、队列管理、速率限制等
3. **维护开销高**：需要跟踪symbol到实例的映射关系

## 简化后的设计

### 1. 移除Symbol绑定

**原来的设计**：
```go
// symbol到实例的映射 - 确保同一symbol总是使用同一实例
symbolToInstance map[string]*PooledWebSocket
```

**简化后的设计**：
```go
// 当前实例索引，用于轮询分配
currentInstanceIndex int
```

### 2. 轮询分配策略

```go
// getAvailableInstance 获取可用的实例（轮询分配）
func (p *WebSocketPool) getAvailableInstance() (*PooledWebSocket, error) {
    // 轮询查找可用实例
    for i := 0; i < len(p.instances); i++ {
        index := (p.currentInstanceIndex + i) % len(p.instances)
        instance := p.instances[index]
        
        if p.isInstanceHealthy(instance) &&
           atomic.LoadInt32(&instance.subscriptions) < maxStreamsPerInstance {
            
            // 更新索引为下一个实例
            p.currentInstanceIndex = (index + 1) % len(p.instances)
            return instance, nil
        }
    }
    // ... 创建新实例逻辑
}
```

### 3. 简化的批量管理器

**原来的复杂设计**：
- 多个配置选项
- 实例ID映射
- 复杂的队列和速率限制

**简化后的设计**：
```go
type SimpleBatchManager struct {
    mu sync.Mutex
    
    // 待处理队列
    pending []string
    
    // 发送函数
    sendFunc func([]string) error
    
    // 批处理定时器
    timer *time.Timer
}
```

## 优化效果

### 1. 负载均衡改进

**原来**：
- Symbol绑定可能导致某些实例过载
- 热门交易对集中在少数实例

**现在**：
- 轮询分配，负载更均匀
- 充分利用所有可用实例

### 2. 代码简化

**原来**：
- 385行的复杂批量管理器
- 复杂的symbol映射管理

**现在**：
- 117行的简单批量管理器
- 无映射关系维护

### 3. 配置简化

**原来**：
```go
type BatchSubscriptionConfig struct {
    MaxBatchSize         int
    BatchDelay           time.Duration
    MaxMessagesPerSecond int
    MessageSendInterval  time.Duration
}
```

**现在**：
```go
const (
    maxBatchSize = 500 // 单次批量订阅的最大stream数量
    batchDelay   = 50  // 批量订阅延迟(毫秒)
)
```

## 功能对比

| 功能 | 原实现 | 简化实现 | 说明 |
|------|--------|----------|------|
| 批量订阅 | ✅ 复杂 | ✅ 简单 | 保持核心功能 |
| 速率限制 | ✅ 精细 | ✅ 基本 | 简化但有效 |
| Symbol绑定 | ✅ | ❌ | 移除不必要的复杂性 |
| 负载均衡 | ❌ 较差 | ✅ 改进 | 轮询分配更均匀 |
| 配置复杂度 | ❌ 高 | ✅ 低 | 减少配置项 |
| 代码维护 | ❌ 复杂 | ✅ 简单 | 代码量减少70% |

## 使用示例

### 基本使用（无变化）

```go
// 用户代码无需修改
wsPool.WatchMiniTicker(ctx, "BTCUSDT", params)
wsPool.WatchTrades(ctx, "ETHUSDT", since, limit, params)
wsPool.WatchOHLCV(ctx, "BNBUSDT", timeframe, since, limit, params)
```

### 批量效果

```go
// 快速连续订阅会被自动批量处理
symbols := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", ...}
for _, symbol := range symbols {
    wsPool.WatchMiniTicker(ctx, symbol, params)
    // 这些订阅会在50ms内被收集并批量发送
}
```

### 监控信息

```go
stats := wsPool.GetPoolStats()
// 输出示例：
// {
//   "total_instances": 2,
//   "connected_instances": 2,
//   "total_subscriptions": 150,
//   "pending_batch_count": 5
// }
```

## 设计权衡

### 优势
1. **代码简洁**：减少70%的代码量
2. **维护简单**：无复杂的映射关系
3. **负载均衡**：轮询分配更公平
4. **性能稳定**：仍保持500/批次的高效率

### 取舍
1. **Symbol一致性**：同一symbol可能分布在不同实例
2. **配置灵活性**：减少了配置选项

### 结论

对于批量订阅场景，简化后的实现：
- ✅ 保持了核心的批量处理能力
- ✅ 显著降低了代码复杂度
- ✅ 改进了负载均衡
- ✅ 更容易理解和维护

这种扁平化的设计更适合现代微服务架构中的"简单优于复杂"原则。