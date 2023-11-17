package pbuckets

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// declare a test groupmember struct that satisfies the GroupMember interface

type testGroupMember struct {
	id     string
	topics []string
	data   []byte
}

func (t testGroupMember) ID() string {
	return t.id
}

func (t testGroupMember) Topics() []string {
	return t.topics
}

func (t testGroupMember) UserData() []byte {
	return t.data
}

// declare a test partition struct that satisfies the Partition interface

type testPartition struct {
	topic string
	id    int
}

func (t testPartition) Topic() string {
	return t.topic
}

func (t testPartition) ID() int {
	return t.id
}

func TestPriorityBucketAssignor(t *testing.T) {
	newPartitions := func(partitionCount int, topics ...string) []Partition {
		partitions := make([]Partition, 0, len(topics)*partitionCount)
		for _, topic := range topics {
			for partition := 0; partition < partitionCount; partition++ {
				partitions = append(partitions, testPartition{
					topic: topic,
					id:    partition,
				})
			}
		}
		return partitions
	}

	tests := map[string]struct {
		Members        []GroupMember
		Partitions     []Partition
		Expected       GroupMemberAssignments
		Topic          string
		Buckets        BucketAllocationPercentageConfig
		ConsumerBucket string
	}{
		"one member, one topic, one partition": {
			Members: []GroupMember{
				testGroupMember{
					id:     "a",
					topics: []string{"topic-1"},
					data:   []byte("High"),
				},
			},
			Partitions: newPartitions(1, "topic-1"),
			Buckets: BucketAllocationPercentageConfig{
				"High": 60, "Low": 40,
			},
			Topic:          "topic-1",
			ConsumerBucket: "High",
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
			},
		},
		"one member, one topic, multiple partitions": {
			Members: []GroupMember{
				testGroupMember{
					id:     "a",
					topics: []string{"topic-1"},
					data:   []byte("Low"),
				},
			},
			Buckets: BucketAllocationPercentageConfig{
				"High": 60, "Low": 40,
			},
			Topic:          "topic-1",
			ConsumerBucket: "Low",
			Partitions:     newPartitions(3, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
			},
		},
		"multiple members, one topic, one partition": {
			Members: []GroupMember{
				testGroupMember{
					id:     "a",
					topics: []string{"topic-1"},
					data:   []byte("High"),
				},
				testGroupMember{
					id:     "b",
					topics: []string{"topic-1"},
				},
			},
			Buckets: BucketAllocationPercentageConfig{
				"High": 60, "Low": 40,
			},
			Topic:          "topic-1",
			ConsumerBucket: "High",
			Partitions:     newPartitions(1, "topic-1"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
				},
				"b": map[string][]int{},
			},
		},
		"multiple members, multiple topics, multiple partitions": {
			Members: []GroupMember{
				testGroupMember{
					id:     "a",
					topics: []string{"topic-1", "topic-2"},
					data:   []byte("Low"),
				},
				testGroupMember{
					id:     "b",
					topics: []string{"topic-2", "topic-3"},
				},
			},
			Buckets: BucketAllocationPercentageConfig{
				"High": 60, "Low": 40,
			},
			Topic:          "topic-1",
			ConsumerBucket: "Low",
			Partitions:     newPartitions(3, "topic-1", "topic-2", "topic-3"),
			Expected: GroupMemberAssignments{
				"a": map[string][]int{
					"topic-1": {0},
					"topic-2": {0},
				},
				"b": map[string][]int{
					"topic-2": {1, 2},
					"topic-3": {0, 1, 2},
				},
			},
		},
	}

	for label, test := range tests {
		t.Run(label, func(t *testing.T) {
			config := PriorityBucketConfig{
				Topic:   test.Topic,
				Buckets: test.Buckets,
			}
			pba, err := NewPriorityBucketAssigner(config, test.ConsumerBucket, nil)
			if err != nil {
				t.Fatal(err)
			}
			assignments := pba.AssignGroups(test.Members, test.Partitions)
			assert.Equal(t, test.Expected, assignments)
		})
	}
}
