提供的主体服务有两个， 一个是tcp server  一个是tcp client 

tcp server 是对接交易所的接口实现， 然后通过协议和tcp client 交互

tcp client 提供交易所接口的便捷调用封装

另外提供两个缓存的db服务， 一个是questdb， 缓存kline trade price 数据
一个是redis ， 缓存 key value 数据， 比如交易所的market数据缓解api 压力等


## 架构重构完成 ✅

### 新架构概览：

1. **server包** - 通用TCP网络框架
   - Transport - 传输层（gnet实现）
   - Router - 消息路由器  
   - Connection - 连接管理
   - Subscription - 订阅机制

2. **core包** - DataHive数据中心引擎
   - 集成了网络服务和交易所业务逻辑
   - 使用server包的框架处理网络通信
   - 包含具体的业务处理器（订阅、查询、状态等）
   - 管理QuestDB和Redis存储连接

3. **client包** - TCP客户端封装
   - 提供便捷的API调用

### 重构成果：

✅ **架构简化**：
- 移除了多余的抽象层
- Spider重命名为DataHive，并整合了所有核心功能
- DataHive直接使用server包框架，避免了重复代码

✅ **职责清晰**：
- server包：纯粹的网络通信框架
- core包：业务逻辑实现
- client包：客户端便捷封装

✅ **编译成功**：
- 修复了所有引用问题
- 项目可以正常编译和运行
- 保留了核心功能的同时简化了结构

### 主要改进：

1. **TCP Server** = server包框架 + core包业务逻辑
2. **TCP Client** = client包的便捷封装  
3. **存储管理** = DataHive内部集成QuestDB + Redis
4. **消息处理** = 统一的路由和处理器机制

### 下一步：
- 可以基于这个简化的架构逐步添加具体的交易所功能
- 恢复和优化备份的业务逻辑文件（miner、db、data_processor）
- 实现具体的数据订阅和处理逻辑