# Stream清理解决方案

## 问题分析

移除了`symbolToInstance`映射后，需要一个机制来管理stream的生命周期：

1. **跟踪stream使用情况**：知道哪些streams正在被使用
2. **清理未使用的streams**：当没有订阅者时自动清理
3. **避免重复订阅**：如果stream已存在，直接复用

## 简化解决方案

### 1. Stream使用计数器
在WebSocket Pool中维护一个简单的使用计数器：

```go
// 简单的stream使用跟踪
streamUsage map[string]int32 // streamName -> 使用计数
```

### 2. 订阅时增加计数
当有新的订阅时：
```go
func (p *WebSocketPool) IncrementStreamUsage(streamName string) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.streamUsage[streamName]++
}
```

### 3. 取消订阅时减少计数
当取消订阅时：
```go
func (p *WebSocketPool) DecrementStreamUsage(streamName string) {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    if count, exists := p.streamUsage[streamName]; exists {
        newCount := count - 1
        if newCount <= 0 {
            // 标记为需要清理
            delete(p.streamUsage, streamName)
            // 异步清理
            go p.cleanupStream(streamName)
        } else {
            p.streamUsage[streamName] = newCount
        }
    }
}
```

### 4. 异步清理
```go
func (p *WebSocketPool) cleanupStream(streamName string) {
    // 延迟30秒后清理，避免频繁订阅/取消订阅
    time.Sleep(30 * time.Second)
    
    p.mu.Lock()
    defer p.mu.Unlock()
    
    // 再次检查是否真的没有使用
    if _, exists := p.streamUsage[streamName]; !exists {
        // 找到对应的实例并取消订阅
        for _, instance := range p.instances {
            if instance.IsConnected() {
                instance.UnsubscribeFromStreams([]string{streamName})
                break
            }
        }
    }
}
```

## 实现优势

1. **简单直接**：只需要一个map和几个简单方法
2. **线程安全**：使用mutex保护共享状态
3. **延迟清理**：避免频繁订阅/取消订阅的性能问题
4. **自动管理**：无需手动清理，自动跟踪使用情况

## 使用场景

这个解决方案特别适合：
- 多个客户端订阅相同streams的场景
- 需要避免重复订阅的场景
- 需要自动清理未使用streams的场景

## 注意事项

1. **延迟清理**：30秒延迟可能需要根据实际需求调整
2. **实例选择**：清理时需要找到正确的实例
3. **错误处理**：清理失败时的处理策略

这个方案平衡了实现复杂度和功能需求，提供了有效的stream生命周期管理。