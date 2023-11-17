package pbuckets

import (
	"encoding/json"
	"math"
	"sync/atomic"
)

const PriorityBucketHeaderKey = "x-kafka-priority-bucket"

type bucket struct {
	name       string
	allocation int
	partitions []int
	counter    uint32
}

type bucketsByAllocation []*bucket

func (a bucketsByAllocation) Len() int           { return len(a) }
func (a bucketsByAllocation) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bucketsByAllocation) Less(i, j int) bool { return a[i].allocation < a[j].allocation }

func (b *bucket) nextPartition() int {
	if len(b.partitions) > 0 {
		nextValue := atomic.AddUint32(&b.counter, 1) - 1
		index := int(nextValue) % len(b.partitions)
		return b.partitions[index]
	}
	return -1
}

func (b *bucket) size(numPartitions int) int {
	return int(math.Round(float64(b.allocation) / 100 * float64(numPartitions)))
}

func (b *bucket) getPartitions() []int {
	return b.partitions
}

func newBucket(name string, allocation int) *bucket {
	return &bucket{
		name:       name,
		allocation: allocation,
	}
}

// BucketAllocationPercentageConfig represents a bucket allocation in a PriorityBucketConfig.
type BucketAllocationPercentageConfig map[string]int

func (b BucketAllocationPercentageConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int(b))
}

func (b *BucketAllocationPercentageConfig) UnmarshalJSON(data []byte) error {
	var m map[string]int
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
	}
	*b = m
	return nil
}

// PriorityBucketConfig represents the configuration of a PriorityBucketPartitioner & PriorityBucketAssigner.
type PriorityBucketConfig struct {
	// Topic is the topic to which the partitioner/assignor will be applied.
	Topic string
	// Buckets is the list of BucketAllocationPercentageConfig to use.
	Buckets BucketAllocationPercentageConfig
}
