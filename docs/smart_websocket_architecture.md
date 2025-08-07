# SmartWebSocketPool 简化架构

## 🎯 设计目标

解决Binance WebSocket限制问题，同时大幅简化架构复杂度。

**Binance限制**：
- 每个连接最多1024个stream
- 每秒最多5条消息
- 每5分钟每IP最多300次连接尝试

## 🏗️ 架构简化

### 之前的复杂架构
```
用户请求
    ↓
WebSocketPool (复杂的符号映射)
    ↓
BatchSubscriptionManager (复杂的实例管理)
    ↓
StreamLifecycleManager (额外的抽象层)
    ↓
WebSocket实例 + StreamManager
```

### 新的简化架构
```
用户请求
    ↓
SmartWebSocketPool (一体化管理)
    ↓
智能连接选择 + 内置批量处理 + 生命周期管理
    ↓
WebSocket实例 + StreamManager
```

## 🧠 SmartWebSocketPool 核心特性

### 1. 统一管理
- **单一职责**：管理所有WebSocket连接和stream
- **内置批量**：自动合并订阅请求，减少消息频率
- **智能路由**：轮询+负载均衡选择最佳连接
- **自动清理**：延迟清理无订阅者的stream

### 2. 限制合规
```go
const (
    maxStreamsPerConnection = 1000 // 安全余量
    maxConnectionsPerPool   = 20   // 最大连接数
    maxMessagesPerSecond    = 4    // 安全余量
    batchSize              = 500   // 批量大小
    batchDelayDuration     = 100ms // 批量延迟
)
```

### 3. 智能批量处理
```go
// 自动触发批量订阅
func (p *SmartWebSocketPool) Subscribe(streamName string) error {
    // 1. 检查是否已订阅（增加计数）
    // 2. 添加到批量队列
    // 3. 触发延迟批处理
    // 4. 遵守速率限制
}
```

### 4. 连接负载均衡
```go
// 智能选择连接
func (p *SmartWebSocketPool) getAvailableConnection() (*PooledConnection, error) {
    // 1. 轮询查找有空间的连接
    // 2. 检查健康状态和stream数量
    // 3. 必要时创建新连接
    // 4. 避免单连接过载
}
```

## 📊 状态管理

### Stream状态跟踪
```go
type StreamState struct {
    name           string                // stream名称
    connection     *PooledConnection    // 所属连接
    subscriberCount int32               // 订阅者计数（原子操作）
    createdAt       time.Time           // 创建时间
    lastUsed        time.Time           // 最后使用时间
}
```

### 连接状态
```go
type PooledConnection struct {
    id          int                    // 连接ID
    ws          *BinanceWebSocket     // WebSocket实例
    streams     map[string]bool       // 该连接的stream
    streamCount int32                 // 原子计数
    isHealthy   bool                  // 健康状态
    createdAt   time.Time            // 创建时间
    lastUsed    time.Time            // 最后使用时间
}
```

## 🔄 工作流程

### 订阅流程
1. **用户调用** `WatchOHLCV()`
2. **构建stream名称** `btcusdt@kline_1m`
3. **检查已有连接** 是否已订阅该stream
4. **选择连接** 轮询+负载均衡
5. **添加到批量队列** 等待批处理
6. **批量发送** 合并多个订阅请求
7. **更新状态** 连接和stream计数

### 批量处理流程
1. **收集待处理** 积累100ms内的订阅请求
2. **速率控制** 确保不超过4条消息/秒
3. **分批发送** 每批最多500个stream
4. **状态更新** 更新连接和stream映射
5. **错误处理** 失败重试或回滚

### 清理流程
1. **订阅者计数减少** 取消订阅时
2. **延迟清理** 30秒后检查是否仍无订阅者
3. **批量取消订阅** 按连接分组处理
4. **状态清理** 移除stream和连接映射
5. **连接优化** 关闭空闲连接

## 🎨 使用示例

### 基本用法
```go
// 创建交易所（自动初始化SmartWebSocketPool）
exchange, _ := binance.New(&binance.Config{EnableWebSocket: true})

// 订阅多个交易对 - 自动批量处理
symbols := []string{"BTC/USDT", "ETH/USDT", "BNB/USDT"}
for _, symbol := range symbols {
    subID, ch, _ := exchange.WatchOHLCV(ctx, symbol, "1m", params)
    // 处理数据...
}

// 获取统计信息
stats := exchange.GetWebSocketPoolStats()
fmt.Printf("连接数: %d, 活跃streams: %d\n", 
    stats["total_connections"], stats["active_streams"])
```

### 高级用法
```go
// 大规模订阅 - 自动多连接管理
symbols := make([]string, 100) // 100个交易对
for _, symbol := range symbols {
    // SmartWebSocketPool会自动：
    // 1. 分配到不同连接（避免1024限制）
    // 2. 批量发送（避免速率限制）
    // 3. 健康监控（自动重连）
    exchange.WatchTrades(ctx, symbol, params)
}
```

## 🛡️ 安全特性

### 限制保护
- **连接数限制**：最多20个连接，避免IP限制
- **Stream数限制**：每连接最多1000个stream
- **速率限制**：每秒最多4条消息，带延迟缓冲
- **批量大小**：每批最多500个stream

### 故障恢复
- **健康检查**：定期检查连接状态
- **自动重连**：连接失败时自动重连
- **状态同步**：重连后恢复订阅状态
- **优雅降级**：部分连接失败不影响整体

### 资源管理
- **延迟清理**：避免频繁订阅/取消订阅
- **内存优化**：及时清理无用状态
- **连接复用**：最大化连接利用率
- **批量操作**：减少系统调用开销

## 📈 性能优化

### 批量优化
- **请求合并**：100ms内的请求合并为一批
- **智能延迟**：根据速率限制动态调整延迟
- **批量大小**：平衡延迟和效率

### 连接优化
- **负载均衡**：轮询分配新stream
- **连接复用**：优先使用现有连接
- **按需创建**：只在必要时创建新连接

### 内存优化
- **原子操作**：高并发下的安全计数
- **延迟清理**：避免频繁内存分配
- **状态压缩**：最小化状态存储

## 🔧 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `maxStreamsPerConnection` | 1000 | 每连接最大stream数 |
| `maxConnectionsPerPool` | 20 | 连接池最大连接数 |
| `maxMessagesPerSecond` | 4 | 最大消息频率 |
| `batchSize` | 500 | 批量订阅大小 |
| `batchDelayDuration` | 100ms | 批量延迟 |
| `cleanupPeriod` | 30s | 清理周期 |

## 🆚 对比优势

| 特性 | 旧架构 | SmartWebSocketPool |
|------|--------|-------------------|
| **复杂度** | 高（多层抽象） | 低（一体化） |
| **性能** | 中等（多次调用） | 高（批量处理） |
| **维护性** | 差（分散逻辑） | 好（集中管理） |
| **可扩展性** | 有限（固定映射） | 强（动态负载均衡） |
| **资源使用** | 高（冗余状态） | 低（精简状态） |
| **故障恢复** | 复杂（多点故障） | 简单（统一处理） |

这个新架构在保持所有原有功能的同时，大幅简化了实现复杂度，提高了性能和可维护性。