package storage

import (
	"errors"
	"fmt"
)

// 存储层错误定义
var (
	ErrConnectionClosed  = errors.New("storage connection is closed")
	ErrConnectionTimeout = errors.New("storage connection timeout")
	ErrStorageNotHealthy = errors.New("storage is not healthy")
	ErrInvalidQuery      = errors.New("invalid query parameters")
	ErrDataNotFound      = errors.New("data not found")
	ErrDuplicateKey      = errors.New("duplicate key")
	ErrTransactionFailed = errors.New("transaction failed")
	ErrBatchSizeTooLarge = errors.New("batch size too large")
	ErrUnsupportedType   = errors.New("unsupported data type")
)

// StorageError 存储错误类型
type StorageError struct {
	Type    string
	Message string
	Cause   error
}

func (e *StorageError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func (e *StorageError) Unwrap() error {
	return e.Cause
}

// 错误创建函数
func ErrInvalidData(message string) error {
	return &StorageError{
		Type:    "INVALID_DATA",
		Message: message,
	}
}

func ErrConnectionError(message string, cause error) error {
	return &StorageError{
		Type:    "CONNECTION_ERROR",
		Message: message,
		Cause:   cause,
	}
}

func ErrQueryError(message string, cause error) error {
	return &StorageError{
		Type:    "QUERY_ERROR",
		Message: message,
		Cause:   cause,
	}
}

func ErrValidationError(message string) error {
	return &StorageError{
		Type:    "VALIDATION_ERROR",
		Message: message,
	}
}

func ErrTimeoutError(message string) error {
	return &StorageError{
		Type:    "TIMEOUT_ERROR",
		Message: message,
	}
}

func ErrNotFoundError(message string) error {
	return &StorageError{
		Type:    "NOT_FOUND_ERROR",
		Message: message,
	}
}

// IsRetryableError 判断错误是否可重试
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// 检查特定的可重试错误
	var storageErr *StorageError
	if errors.As(err, &storageErr) {
		switch storageErr.Type {
		case "CONNECTION_ERROR", "TIMEOUT_ERROR", "QUERY_ERROR":
			return true
		default:
			return false
		}
	}

	// 检查标准错误
	return errors.Is(err, ErrConnectionTimeout) ||
		errors.Is(err, ErrConnectionClosed) ||
		errors.Is(err, ErrStorageNotHealthy)
}

// IsValidationError 判断是否为验证错误
func IsValidationError(err error) bool {
	if err == nil {
		return false
	}

	var storageErr *StorageError
	if errors.As(err, &storageErr) {
		return storageErr.Type == "VALIDATION_ERROR" || storageErr.Type == "INVALID_DATA"
	}

	return false
}
