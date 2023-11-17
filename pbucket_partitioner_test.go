package pbuckets

import (
	"testing"
)

type Message struct {
	Headers map[string]string
	Topic   string
}

func TestPriorityBucketPartitioner(t *testing.T) {
	testCases := map[string]struct {
		BucketHeaders      [][]byte
		Partitions         [][]int
		AssignedPartitions []int
	}{
		"single message, less partitions": {
			BucketHeaders: [][]byte{
				[]byte("High"),
			},
			Partitions: [][]int{
				{0, 1, 2},
			},
			AssignedPartitions: []int{1},
		},
		"multiple message, less partitions": {
			BucketHeaders: [][]byte{
				[]byte("Low"),
				[]byte("High"),
				[]byte("High"),
			},
			Partitions: [][]int{
				{0, 1, 2},
				{0, 1, 2},
				{0, 1, 2},
			},
			AssignedPartitions: []int{0, 1, 2},
		},
		"single message, adequate partitions": {
			BucketHeaders: [][]byte{
				[]byte("High"),
			},
			Partitions: [][]int{
				{0, 1, 2, 3, 4},
			},
			AssignedPartitions: []int{2},
		},
		"multiple messages, no partition change": {
			BucketHeaders: [][]byte{
				[]byte("High"),
				[]byte("High"),
				[]byte("High"),
				[]byte("Low"),
			},
			Partitions: [][]int{
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
			},
			AssignedPartitions: []int{2, 3, 4, 0},
		},

		"partition gained": {
			BucketHeaders: [][]byte{
				[]byte("High"),
				[]byte("High"),
				[]byte("Low"),
			},
			Partitions: [][]int{
				{0, 1, 2},
				{0, 1, 2},
				{0, 1, 2, 3, 4},
			},
			AssignedPartitions: []int{1, 2, 0},
		},
		"partition lost": {
			BucketHeaders: [][]byte{
				[]byte("High"),
				[]byte("High"),
				[]byte("Low"),
			},
			Partitions: [][]int{
				{0, 1, 2, 3, 4},
				{0, 1, 2, 3, 4},
				{0, 1, 2},
			},
			AssignedPartitions: []int{2, 3, 0},
		},
	}

	for label, test := range testCases {
		t.Run(label, func(t *testing.T) {
			config := PriorityBucketConfig{
				Topic: "test",
				Buckets: BucketAllocationPercentageConfig{
					"High": 60, "Low": 40,
				},
			}
			// declare variable for MessageHeaderFetcherFunc
			messageHeaderFunc := func(msg any, key string) (string, bool) {
				message := msg.(Message)
				value, ok := message.Headers[key]
				return value, ok
			}
			// declare variable for MessageTopicFetcherFunc
			messageTopicFunc := func(msg any) string {
				message := msg.(Message)
				return message.Topic
			}
			pbp, err := NewPriorityBucketPartitioner(config, nil, messageHeaderFunc, messageTopicFunc)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			var partition int
			for i, header := range test.BucketHeaders {
				msg := Message{
					Headers: map[string]string{
						PriorityBucketHeaderKey: string(header),
					},
					Topic: "test",
				}
				partition = pbp.Balance(msg, test.Partitions[i]...)
				if partition != test.AssignedPartitions[i] {
					t.Log("label = ", label, "| bucket = ", string(header))
					t.Errorf("expected %v; got %v", test.AssignedPartitions[i], partition)
				}
			}
		})
	}
}
