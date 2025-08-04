package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"datahive/pkg/protocol/pb"

	"google.golang.org/protobuf/proto"
)

// MessageBuilder 消息构建器，提供便捷的消息创建方法
type MessageBuilder struct{}

// NewBuilder 创建消息构建器
func NewBuilder() *MessageBuilder {
	return &MessageBuilder{}
}

// NewRequest 创建请求消息
func (b *MessageBuilder) NewRequest(id string, action pb.ActionType, data proto.Message) (*pb.Message, error) {
	payload, err := b.marshalData(data)
	if err != nil {
		return nil, err
	}

	return &pb.Message{
		Id:        id,
		Action:    action,
		Type:      pb.MessageType_REQUEST,
		Data:      payload,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// NewResponse 创建响应消息
func (b *MessageBuilder) NewResponse(id string, action pb.ActionType, data proto.Message) (*pb.Message, error) {
	payload, err := b.marshalData(data)
	if err != nil {
		return nil, err
	}

	return &pb.Message{
		Id:        id,
		Action:    action,
		Type:      pb.MessageType_RESPONSE,
		Data:      payload,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// NewNotification 创建通知消息
func (b *MessageBuilder) NewNotification(action pb.ActionType, data proto.Message) (*pb.Message, error) {
	payload, err := b.marshalData(data)
	if err != nil {
		return nil, err
	}

	return &pb.Message{
		Id:        generateMessageID(),
		Action:    action,
		Type:      pb.MessageType_NOTIFICATION,
		Data:      payload,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// NewError 创建错误消息
func (b *MessageBuilder) NewError(id string, code int32, message, details string) (*pb.Message, error) {
	errorData := &pb.Error{
		Code:    code,
		Message: message,
		Details: details,
	}

	payload, err := b.marshalData(errorData)
	if err != nil {
		return nil, err
	}

	return &pb.Message{
		Id:        id,
		Type:      pb.MessageType_ERROR,
		Data:      payload,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// UnmarshalData 解析消息数据
func (b *MessageBuilder) UnmarshalData(msg *pb.Message, target proto.Message) error {
	return proto.Unmarshal(msg.Data, target)
}

// marshalData 序列化数据
func (b *MessageBuilder) marshalData(data proto.Message) ([]byte, error) {
	if data == nil {
		return nil, nil
	}
	return proto.Marshal(data)
}

// MarshalMessage 序列化消息
func (b *MessageBuilder) MarshalMessage(msg *pb.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

// UnmarshalMessage 反序列化消息
func (b *MessageBuilder) UnmarshalMessage(data []byte, msg *pb.Message) error {
	return proto.Unmarshal(data, msg)
}

// generateMessageID 生成消息ID
func generateMessageID() string {
	// 使用时间戳 + 随机字节生成唯一ID
	timestamp := time.Now().UnixNano()

	// 生成4字节随机数
	randBytes := make([]byte, 4)
	rand.Read(randBytes)

	return fmt.Sprintf("%d-%s", timestamp, hex.EncodeToString(randBytes))
}

// GenerateID 公开的ID生成函数
func GenerateID() string {
	return generateMessageID()
}
