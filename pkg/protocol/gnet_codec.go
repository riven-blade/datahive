package protocol

import (
	"fmt"
	"sync"

	"github.com/riven-blade/datahive/pkg/protocol/pb"
	"google.golang.org/protobuf/proto"
)

// GNetCodec gnet协议编解码器
// 专为gnet异步事件驱动架构设计，支持流式TCP消息处理
type GNetCodec struct {
	maxMessageSize uint32
}

// NewGNetCodec 创建gnet协议编解码器
func NewGNetCodec() *GNetCodec {
	return &GNetCodec{
		maxMessageSize: 10 * 1024 * 1024, // 10MB 默认限制
	}
}

// SetMaxMessageSize 设置最大消息大小
func (c *GNetCodec) SetMaxMessageSize(size uint32) {
	c.maxMessageSize = size
}

// EncodeMessage 编码消息为gnet传输格式：4字节长度前缀 + protobuf数据
func (c *GNetCodec) EncodeMessage(msg *pb.Message) ([]byte, error) {
	// 序列化protobuf消息
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}

	// 检查消息大小
	if uint32(len(data)) > c.maxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max: %d)", len(data), c.maxMessageSize)
	}

	// 创建带长度前缀的完整消息
	totalLen := 4 + len(data)
	result := make([]byte, totalLen)

	// 写入长度前缀（4字节大端序）
	result[0] = byte(len(data) >> 24)
	result[1] = byte(len(data) >> 16)
	result[2] = byte(len(data) >> 8)
	result[3] = byte(len(data))

	// 写入消息数据
	copy(result[4:], data)

	return result, nil
}

// MessageBuffer gnet消息缓冲区
// 支持TCP流式传输的消息分包/粘包处理
type MessageBuffer struct {
	codec        *GNetCodec
	buffer       []byte
	expectedLen  uint32
	headerParsed bool
	mu           sync.Mutex
}

// NewMessageBuffer 创建消息缓冲区
func NewMessageBuffer(codec *GNetCodec) *MessageBuffer {
	if codec == nil {
		codec = NewGNetCodec()
	}
	return &MessageBuffer{
		codec:  codec,
		buffer: make([]byte, 0, 8192), // 8KB 初始容量
	}
}

// ProcessData 处理接收到的数据，返回解析出的完整消息列表
func (mb *MessageBuffer) ProcessData(data []byte) ([]*pb.Message, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	// 将新数据添加到缓冲区
	mb.buffer = append(mb.buffer, data...)

	var messages []*pb.Message

	// 循环处理缓冲区中的完整消息
	for {
		msg, processed, err := mb.processBuffer()
		if err != nil {
			return nil, err
		}
		if !processed {
			break // 没有更多完整消息可处理
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// processBuffer 处理缓冲区中的一条消息
func (mb *MessageBuffer) processBuffer() (*pb.Message, bool, error) {
	// 如果还没有解析消息头，尝试解析消息长度
	if !mb.headerParsed {
		if len(mb.buffer) < 4 {
			return nil, false, nil // 需要更多数据来读取消息头
		}

		// 解析消息长度（4字节大端序）
		mb.expectedLen = uint32(mb.buffer[0])<<24 |
			uint32(mb.buffer[1])<<16 |
			uint32(mb.buffer[2])<<8 |
			uint32(mb.buffer[3])
		mb.headerParsed = true

		// 检查消息长度是否合理
		if mb.expectedLen == 0 {
			return nil, false, fmt.Errorf("invalid message length: 0")
		}
		if mb.expectedLen > mb.codec.maxMessageSize {
			return nil, false, fmt.Errorf("message too large: %d bytes (max: %d)", mb.expectedLen, mb.codec.maxMessageSize)
		}
	}

	// 检查是否有完整的消息
	totalLen := 4 + mb.expectedLen
	if len(mb.buffer) < int(totalLen) {
		return nil, false, nil // 需要更多数据
	}

	// 提取消息数据
	messageData := mb.buffer[4:totalLen]

	// 解析protobuf消息
	var msg pb.Message
	if err := proto.Unmarshal(messageData, &msg); err != nil {
		return nil, false, fmt.Errorf("unmarshal message: %w", err)
	}

	// 移除已处理的数据
	mb.buffer = mb.buffer[totalLen:]
	mb.headerParsed = false
	mb.expectedLen = 0

	return &msg, true, nil
}

// Clear 清空缓冲区
func (mb *MessageBuffer) Clear() {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.buffer = mb.buffer[:0]
	mb.headerParsed = false
	mb.expectedLen = 0
}

// BufferSize 获取当前缓冲区大小
func (mb *MessageBuffer) BufferSize() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.buffer)
}
