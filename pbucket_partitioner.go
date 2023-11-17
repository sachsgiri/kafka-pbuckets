package pbuckets

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/emirpasic/gods/maps/linkedhashmap"
)

// MessageHeaderFetcherFunc is a function that fetches a header from a kafka message
type MessageHeaderFetcherFunc func(message any, key string) (string, bool)

// MessageTopicFetcherFunc is a function that fetches a topic from a kafka message
type MessageTopicFetcherFunc func(message any) string

// PriorityBucketPartitioner is a kafka.Partitioner that assigns partitions to consumers based on a priority bucket.
type PriorityBucketPartitioner struct {
	fallback             Partitioner
	config               PriorityBucketConfig
	lastBucket           string
	buckets              *linkedhashmap.Map
	lastPartitionCount   int
	mutex                sync.Mutex
	messageHeaderFetcher MessageHeaderFetcherFunc
	messageTopicFetcher  MessageTopicFetcherFunc
}

// NewPriorityBucketPartitioner creates a new PriorityBucketPartitioner.
func NewPriorityBucketPartitioner(config PriorityBucketConfig, fallbackPartitioner Partitioner, fetcherFunc MessageHeaderFetcherFunc, topicFetcher MessageTopicFetcherFunc) (*PriorityBucketPartitioner, error) {
	sumAllocations := 0
	for _, allocation := range config.Buckets {
		sumAllocations += allocation
	}
	if sumAllocations != 100 {
		return nil, errors.New("the sum of all bucket allocations must be 100")
	}

	if fallbackPartitioner == nil {
		fallbackPartitioner = &RoundRobinPartitioner{}
	}

	bp := &PriorityBucketPartitioner{
		fallback:             fallbackPartitioner,
		config:               config,
		buckets:              linkedhashmap.New(),
		messageHeaderFetcher: fetcherFunc,
		messageTopicFetcher:  topicFetcher,
	}

	var buckets bucketsByAllocation
	for bucketName, allocation := range config.Buckets {
		bucketName := strings.TrimSpace(bucketName)
		buckets = append(buckets, newBucket(bucketName, allocation))
	}

	sort.Sort(buckets)

	for _, bucket := range buckets {
		bp.buckets.Put(bucket.name, bucket)
	}

	return bp, nil
}

// Balance returns the partition to which the message should be sent based on the priority bucket.
func (p *PriorityBucketPartitioner) Balance(msg any, partitions ...int) (partition int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.config.Topic == "" || p.config.Topic != p.messageTopicFetcher(msg) {
		return p.fallback.Balance(msg, partitions...)
	}

	var bucketName string

	if bucketInHeader, ok := p.messageHeaderFetcher(msg, PriorityBucketHeaderKey); ok {
		bucketName = bucketInHeader
	}

	if bucketName == "" {
		return p.fallback.Balance(msg, partitions...)
	}

	found, ok := p.buckets.Get(bucketName)
	if !ok {
		return p.fallback.Balance(msg, partitions...)
	}

	p.lastBucket = bucketName
	partitionCount := len(partitions)
	if p.lastPartitionCount != partitionCount {
		err := updatePartitionsAssignment(p.buckets, partitions)
		if err != nil {
			return p.fallback.Balance(msg, partitions...)
		}
		p.lastPartitionCount = partitionCount
	}

	return found.(*bucket).nextPartition()
}

// updatePartitionsAssignment updates the partitions assignment for each bucket.
func updatePartitionsAssignment(buckets *linkedhashmap.Map, partitions []int) error {
	numBuckets := buckets.Size()
	numPartitions := len(partitions)

	if numPartitions < numBuckets {
		message := fmt.Sprintf("the number of partitions needs to be at least %d", numBuckets)
		return errors.New(message)
	}

	// Design the layout of the distribution of partitions
	// across the buckets.
	distribution := 0
	layout := make(map[string]int)
	it := buckets.Iterator()
	for it.Begin(); it.Next(); {
		bucketName := it.Key().(string)
		bucket := it.Value().(*bucket)
		bucketSize := bucket.size(len(partitions))
		layout[bucketName] = bucketSize
		distribution += bucketSize
	}

	// Check if there are unassigned partitions.
	// If so then distribute them over the buckets
	// starting from the top to bottom until there
	// are no partitions left.
	remaining := len(partitions) - distribution
	if remaining > 0 {
		counter := -1
		availableBuckets := make([]string, 0)
		for it.Begin(); it.Next(); {
			availableBuckets = append(availableBuckets, it.Key().(string))
		}
		for remaining > 0 {
			counter++
			index := int(math.Abs(float64(counter))) % len(availableBuckets)
			bucketName := availableBuckets[index]
			bucketSize := layout[bucketName]
			layout[bucketName] = bucketSize + 1
			remaining--
		}
	}

	// Finally assign the available partitions to buckets
	partition := -1
	for it.Begin(); it.Next(); {
		bucketName := it.Key().(string)
		bucket := it.Value().(*bucket)
		bucketSize := layout[bucketName]
		bucket.partitions = make([]int, 0)
		for i := 0; i < bucketSize; i++ {
			partition++
			bucket.partitions = append(bucket.partitions, partition)
			if partition == len(partitions)-1 {
				break
			}
		}
	}
	return nil
}
