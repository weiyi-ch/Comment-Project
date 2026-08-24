package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// writeByConn 基于底层 Conn 向指定 partition 写入 Kafka 消息。
//
// 该函数用于理解 Kafka 直连 Leader 的写入方式，实际业务更常使用 Writer。
func writeByConn() {
	topic := "my-topic"
	partition := 0

	// 连接至Kafka集群的Leader节点
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	// 设置发送消息的超时时间
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// 发送消息
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	// 关闭连接
	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

// writeBywriter 基于 kafka.Writer 写入消息，适合日常生产者代码。
func writeBywriter() {
	// 创建一个writer 向topic-A发送消息
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:        "topic-A",
		Balancer:     &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks: kafka.RequireAll,    // ack模式
		Async:        true,                // 异步
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}

// main 运行 Kafka producer demo。
func main() {
	writeBywriter()
}
