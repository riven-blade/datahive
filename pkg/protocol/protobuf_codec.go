package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
)

// Codec protobuf编解码器
type Codec struct {
	conn           io.ReadWriter
	reader         *bufio.Reader
	readTimeout    time.Duration
	writeTimeout   time.Duration
	maxMessageSize int
}

// NewCodec 创建protobuf编解码器
func NewCodec(conn io.ReadWriter) *Codec {
	return &Codec{
		conn:           conn,
		reader:         bufio.NewReader(conn),
		readTimeout:    120 * time.Second,
		writeTimeout:   30 * time.Second,
		maxMessageSize: 10 * 1024 * 1024, // 10MB
	}
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

// SetCompressionLevel 保持接口兼容，protobuf版本暂不支持
func (c *Codec) SetCompressionLevel(level int) {
	// protobuf版本暂不支持压缩
}

// WriteMessage 写入protobuf消息
func (c *Codec) WriteMessage(msg *Message) error {
	// 序列化消息
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// 设置写超时
	if netConn, ok := c.conn.(net.Conn); ok {
		netConn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	// 写入长度前缀（4字节big-endian）+ 数据
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(data)))

	if _, err := c.conn.Write(lengthBytes); err != nil {
		return fmt.Errorf("write length prefix: %w", err)
	}

	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("write message data: %w", err)
	}

	return nil
}

// ReadMessage 读取protobuf消息
func (c *Codec) ReadMessage() (*Message, error) {
	// 设置读超时
	if netConn, ok := c.conn.(net.Conn); ok {
		netConn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	// 读取长度前缀（4字节）
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, lengthBytes); err != nil {
		return nil, fmt.Errorf("read length prefix: %w", err)
	}

	// 解析消息长度
	messageLength := binary.BigEndian.Uint32(lengthBytes)
	if messageLength > uint32(c.maxMessageSize) {
		return nil, fmt.Errorf("message too large: %d bytes (max: %d)", messageLength, c.maxMessageSize)
	}

	// 读取消息数据
	data := make([]byte, messageLength)
	if _, err := io.ReadFull(c.reader, data); err != nil {
		return nil, fmt.Errorf("read message data: %w", err)
	}

	// 反序列化消息
	var msg Message
	if err := proto.Unmarshal(data, &msg); err != nil {
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
