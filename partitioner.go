package pbuckets

import "sync/atomic"

// The Partitioner interface provides an abstraction of the message distribution
// logic used by Writer instances to route messages to the partitions available
// on a kafka cluster.
//
// Partitioner must be safe to use concurrently from multiple goroutines.
type Partitioner interface {
	// Balance receives a message and a set of available partitions and
	// returns the partition number that the message should be routed to.
	//
	// An application should refrain from using a balancer to manage multiple
	// sets of partitions (from different topics for examples), use one balancer
	// instance for each partition set, so the balancer can detect when the
	// partitions change and assume that the kafka topic has been rebalanced.
	Balance(msg any, partitions ...int) (partition int)
}

// RoundRobinPartitioner is a Partitioner implementation that equally distributes messages
// across all available partitions.
type RoundRobinPartitioner struct {
	// Use a 32 bits integer so RoundRobin values don't need to be aligned to
	// apply atomic increments.
	offset uint32
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobinPartitioner) Balance(msg any, partitions ...int) int {
	return rr.balance(partitions)
}

func (rr *RoundRobinPartitioner) balance(partitions []int) int {
	length := uint32(len(partitions))
	offset := atomic.AddUint32(&rr.offset, 1) - 1
	return partitions[offset%length]
}
