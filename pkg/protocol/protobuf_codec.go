package protocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/riven-blade/datahive/pkg/protocol/pb"

	"google.golang.org/protobuf/proto"
)

// Codec protobuf编解码器
type Codec struct {
	conn           io.ReadWriter
	reader         *bufio.Reader
	readTimeout    time.Duration
	writeTimeout   time.Duration
	maxMessageSize int

	// 缓冲区池，减少内存分配
	bufferPool sync.Pool
}

// NewCodec 创建protobuf编解码器
func NewCodec(conn io.ReadWriter) *Codec {
	c := &Codec{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		readTimeout:    120 * time.Second,
		writeTimeout:   30 * time.Second,
		maxMessageSize: 10 * 1024 * 1024, // 10MB
	}

	// 初始化缓冲区池
	c.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024) // 1KB初始容量
		},
	}

	return c
}

// SetReadTimeout 设置读超时
func (c *Codec) SetReadTimeout(timeout time.Duration) {
	c.readTimeout = timeout
}

// SetWriteTimeout 设置写超时
func (c *Codec) SetWriteTimeout(timeout time.Duration) {
	c.writeTimeout = timeout
}

// SetMaxMessageSize 设置最大消息大小
func (c *Codec) SetMaxMessageSize(size int) {
	c.maxMessageSize = size
}

// WriteMessage 写入protobuf消息
func (c *Codec) WriteMessage(msg *pb.Message) error {
	return c.WriteMessageWithContext(context.Background(), msg)
}

// WriteMessageWithContext 带上下文的写入protobuf消息
func (c *Codec) WriteMessageWithContext(ctx context.Context, msg *pb.Message) error {
	// 序列化消息
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// 设置写超时
	if netConn, ok := c.conn.(net.Conn); ok {
		deadline := time.Now().Add(c.writeTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		netConn.SetWriteDeadline(deadline)
	}

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 使用缓冲区池减少内存分配
	buffer := c.bufferPool.Get().([]byte)
	buffer = buffer[:0] // 重置长度但保留容量
	defer c.bufferPool.Put(buffer)

	// 确保缓冲区有足够的容量
	needed := 4 + len(data)
	if cap(buffer) < needed {
		buffer = make([]byte, 0, needed)
	}

	// 组装消息：长度前缀（4字节big-endian）+ 数据
	buffer = buffer[:4]
	binary.BigEndian.PutUint32(buffer, uint32(len(data)))
	buffer = append(buffer, data...)

	// 一次性写入完整消息
	if _, err := c.conn.Write(buffer); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return nil
}

// ReadMessage 读取protobuf消息
func (c *Codec) ReadMessage() (*pb.Message, error) {
	return c.ReadMessageWithContext(context.Background())
}

// ReadMessageWithContext 带上下文的读取protobuf消息
func (c *Codec) ReadMessageWithContext(ctx context.Context) (*pb.Message, error) {
	// 设置读超时
	if netConn, ok := c.conn.(net.Conn); ok {
		deadline := time.Now().Add(c.readTimeout)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		netConn.SetReadDeadline(deadline)
	}

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// 读取长度前缀（4字节）
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, lengthBytes); err != nil {
		return nil, fmt.Errorf("read length prefix: %w", err)
	}

	// 解析消息长度
	messageLength := binary.BigEndian.Uint32(lengthBytes)
	if messageLength == 0 {
		return nil, fmt.Errorf("invalid message length: 0")
	}
	if messageLength > uint32(c.maxMessageSize) {
		return nil, fmt.Errorf("message too large: %d bytes (max: %d)", messageLength, c.maxMessageSize)
	}

	// 使用缓冲区池读取消息数据
	buffer := c.bufferPool.Get().([]byte)
	defer c.bufferPool.Put(buffer)

	// 确保缓冲区有足够的容量
	if cap(buffer) < int(messageLength) {
		buffer = make([]byte, messageLength)
	} else {
		buffer = buffer[:messageLength]
	}

	if _, err := io.ReadFull(c.reader, buffer); err != nil {
		return nil, fmt.Errorf("read message data: %w", err)
	}

	// 反序列化消息
	var msg pb.Message
	if err := proto.Unmarshal(buffer, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}

	return &msg, nil
}

// Close 关闭编解码器
func (c *Codec) Close() error {
	if closer, ok := c.conn.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
