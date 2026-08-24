package data

// redisValueBytes 兼容 go-redis MGET 返回的 string 和 []byte。
func redisValueBytes(value interface{}) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}
