package utils

import (
	"unicode/utf8"
)

// SanitizeUTF8 清理字符串中的无效UTF-8字符
func SanitizeUTF8(s string) string {
	// 如果字符串是有效的UTF-8，直接返回
	if utf8.ValidString(s) {
		return s
	}

	// 清理无效的UTF-8字符，替换为空字符串
	result := make([]byte, 0, len(s))
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			// 跳过无效字节
			s = s[1:]
		} else {
			// 保留有效字符
			result = append(result, s[:size]...)
			s = s[size:]
		}
	}

	return string(result)
}

// IsValidUTF8 检查字符串是否为有效的UTF-8
func IsValidUTF8(s string) bool {
	return utf8.ValidString(s)
}

// SanitizeUTF8Map 批量清理map中所有字符串值的UTF-8字符
func SanitizeUTF8Map(m map[string]string) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[SanitizeUTF8(k)] = SanitizeUTF8(v)
	}
	return result
}

// SanitizeUTF8Slice 批量清理slice中所有字符串的UTF-8字符
func SanitizeUTF8Slice(slice []string) []string {
	result := make([]string, len(slice))
	for i, s := range slice {
		result[i] = SanitizeUTF8(s)
	}
	return result
}
