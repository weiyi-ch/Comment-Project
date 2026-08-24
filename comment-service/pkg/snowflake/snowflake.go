package snowflake

import (
	"sync"
	"time"
)

// 雪花算法位分配常量，决定 ID 中时间戳、机器号和序列号三部分的布局。
const (
	// 起始时间戳 (2026-04-09 00:00:00)
	// 一旦确定请勿修改，否则会产生重复 ID
	epoch int64 = 1775664000000

	// 各个部分占用的位数
	workerIDBits uint8 = 10 // 机器标识占用的位数
	sequenceBits uint8 = 12 // 序列号占用的位数

	// 各个部分的最大值 (位运算计算)
	maxWorkerID int64 = -1 ^ (-1 << workerIDBits) // 1023
	maxSequence int64 = -1 ^ (-1 << sequenceBits) // 4095

	// 需要位移的偏移量
	workerIDShift  uint8 = sequenceBits
	timestampShift uint8 = sequenceBits + workerIDBits
)

// Node 定义一个雪花算法节点。
//
// 该节点在单进程内通过互斥锁保证同一毫秒内 sequence 单调递增。
type Node struct {
	mu        sync.Mutex
	timestamp int64
	workerID  int64
	sequence  int64
}

// node 是当前服务进程默认使用的雪花节点。
var node = &Node{
	timestamp: 0,
	workerID:  1,
	sequence:  0,
}

// Generate 生成一个新的雪花 ID。
//
// ID 由时间戳、workerID 和毫秒内序列号拼接而成，适合用作业务侧唯一 ID。
func (n *Node) Generate() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	now := time.Now().UnixMilli()

	if now == n.timestamp {
		// 如果在同一毫秒内，序列号递增
		n.sequence = (n.sequence + 1) & maxSequence

		// 如果当前毫秒内的序列号用完，等待下一毫秒
		if n.sequence == 0 {
			for now <= n.timestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		// 如果是新的毫秒，序列号重置为 0
		n.sequence = 0
	}

	n.timestamp = now

	// 核心逻辑：位运算拼接
	// (当前时间-起始时间) 左移到高位 | 机器ID左移到中间 | 序列号在低位
	id := ((now - epoch) << timestampShift) |
		(n.workerID << workerIDShift) |
		(n.sequence)

	return id
}

// GenID 使用默认节点生成业务 ID。
func GenID() int64 {
	return node.Generate()
}
