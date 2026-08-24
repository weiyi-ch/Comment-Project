package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// readByReader 通过 kafka.Reader 持续消费指定 topic 的消息。
func readByReader() {
	// 创建Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "comment-service",
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(0) // 设置Offset

	// 接收消息
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	// 程序退出前关闭Reader
	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

// main 运行 Kafka consumer demo。
func main() {
	readByReader()
}
