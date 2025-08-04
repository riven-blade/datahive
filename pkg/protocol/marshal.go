package protocol

import (
	"time"

	"google.golang.org/protobuf/proto"
)

// Marshal 序列化protobuf消息
func Marshal(msg *Message) ([]byte, error) {
	return proto.Marshal(msg)
}

// Unmarshal 反序列化protobuf消息
func Unmarshal(data []byte, msg *Message) error {
	return proto.Unmarshal(data, msg)
}

// NewRequestMessage 创建请求消息
func NewRequestMessage(action ActionType, payload []byte) *Message {
	return &Message{
		Id:        generateMessageID(),
		Action:    action,
		Type:      TypeRequest,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewResponseMessage 创建响应消息
func NewResponseMessage(action ActionType, requestID string, payload []byte) *Message {
	return &Message{
		Id:        requestID,
		Action:    action,
		Type:      TypeResponse,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewNotificationMessage 创建通知消息
func NewNotificationMessage(action ActionType, payload []byte) *Message {
	return &Message{
		Id:        generateMessageID(),
		Action:    action,
		Type:      TypeNotification,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewErrorMessage 创建错误消息
func NewErrorMessage(requestID string, errorCode int32, errorMessage string) (*Message, error) {
	errorMsg := &Error{
		Code:    errorCode,
		Message: errorMessage,
	}

	errorPayload, err := proto.Marshal(errorMsg)
	if err != nil {
		return nil, err
	}

	return &Message{
		Id:        requestID,
		Action:    ActionTickerUpdate, // 使用默认action
		Type:      TypeError,
		Payload:   errorPayload,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// generateMessageID 生成消息ID
func generateMessageID() string {
	return time.Now().Format("20060102150405") + "." + string(rune(time.Now().Nanosecond()%1000000))
}
