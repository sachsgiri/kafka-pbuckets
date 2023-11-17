package pbuckets

import (
	"errors"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/emirpasic/gods/maps/linkedhashmap"
)

// PriorityBucketAssigner is a GroupBalancer that assigns partitions to consumers based on a priority bucket.
// It uses a fallback GroupBalancer to assign partitions to consumers that are not in the same bucket.
// It implements the GroupBalancer interface.
type PriorityBucketAssigner struct {
	fallback           GroupBalancer
	config             PriorityBucketConfig
	buckets            *linkedhashmap.Map
	lastPartitionCount int
	consumerBucket     string
}

// UserData returns the consumer bucket as the user data.
func (bpa *PriorityBucketAssigner) UserData() ([]byte, error) {
	return []byte(bpa.consumerBucket), nil
}

// ProtocolName returns the name of the protocol used by this assignor.
func (bpa *PriorityBucketAssigner) ProtocolName() string {
	return "priority-bucket"
}

// NewPriorityBucketAssigner creates a new PriorityBucketAssigner.
// fallbackBalancer is the fallback GroupBalancer to use when a consumer is not in the same bucket.
// If nil, RoundRobinGroupBalancer is used.
func NewPriorityBucketAssigner(config PriorityBucketConfig, consumerBucket string, fallbackBalancer GroupBalancer) (*PriorityBucketAssigner, error) {
	sumAllocations := 0
	for _, allocation := range config.Buckets {
		sumAllocations += allocation
	}
	if sumAllocations != 100 {
		return nil, errors.New("the sum of all bucket allocations must be 100")
	}

	if consumerBucket == "" {
		return nil, errors.New("the consumer bucket must be set")
	}

	if fallbackBalancer == nil {
		fallbackBalancer = RoundRobinGroupBalancer{}
	}

	ba := &PriorityBucketAssigner{
		fallback:       fallbackBalancer,
		config:         config,
		consumerBucket: consumerBucket,
		buckets:        linkedhashmap.New(),
	}

	var buckets bucketsByAllocation
	for bucketName, allocation := range config.Buckets {
		bucketName := strings.TrimSpace(bucketName)
		buckets = append(buckets, newBucket(bucketName, allocation))
	}

	sort.Sort(buckets)

	for _, bucket := range buckets {
		ba.buckets.Put(bucket.name, bucket)
	}

	return ba, nil
}

// AssignGroups assigns partitions to consumers based on a priority bucket.
func (bpa *PriorityBucketAssigner) AssignGroups(members []GroupMember, topicPartitions []Partition) GroupMemberAssignments {
	groupAssignments := GroupMemberAssignments{}

	membersByBucketTopic, membersByNonBucketTopic := bpa.filterMembersByBucketTopic(members)
	partitionsByBucketTopic, partitionsByNonBucketTopic := bpa.filterPartitionsByBucketTopic(topicPartitions)

	if bpa.lastPartitionCount != len(partitionsByBucketTopic) {
		err := bpa.updateBucketPartitions(partitionsByBucketTopic)
		if err != nil {
			return bpa.fallback.AssignGroups(members, topicPartitions)
		}
		bpa.lastPartitionCount = len(partitionsByBucketTopic)
	}

	membersByBucket := bpa.getGroupMembersByBucket(membersByBucketTopic)

	// Evenly distribute (round robbin) the partitions across the
	// available consumers in per-bucket basis.
	var counter int32
	counter = -1
	it := bpa.buckets.Iterator()
	for it.Begin(); it.Next(); {
		bucketName := it.Key().(string)
		bucket := it.Value().(*bucket)
		consumersPerBucket, ok := membersByBucket[bucketName]
		if !ok {
			continue
		}
		for _, partition := range bucket.getPartitions() {
			nextValue := atomic.AddInt32(&counter, 1)
			index := int(nextValue) % len(consumersPerBucket)
			memberID := consumersPerBucket[index]
			assignmentsByTopic, ok := groupAssignments[memberID]
			if !ok {
				assignmentsByTopic = map[string][]int{}
			}
			assignmentsByTopic[bpa.config.Topic] = append(assignmentsByTopic[bpa.config.Topic], partition)
			groupAssignments[memberID] = assignmentsByTopic
		}
	}

	// Merge the assignments from the fallback balancer with the assignments from the bucket balancer.
	bpa.mergeWithNonTopicPartitions(membersByNonBucketTopic, partitionsByNonBucketTopic, groupAssignments)

	return groupAssignments
}

// getGroupMembersByBucket returns a map of bucket name to a list of consumer IDs.
func (bpa *PriorityBucketAssigner) getGroupMembersByBucket(membersByBucketTopic []GroupMember) map[string][]string {
	membersPerBucket := map[string][]string{}
	for _, member := range membersByBucketTopic {
		bucketName := string(member.UserData())
		_, ok := bpa.buckets.Get(bucketName)
		if !ok {
			continue
		}
		consumersPerBucket, ok := membersPerBucket[bucketName]
		if !ok {
			consumersPerBucket = []string{}
		}
		consumersPerBucket = append(consumersPerBucket, member.ID())
		membersPerBucket[bucketName] = consumersPerBucket
	}
	return membersPerBucket
}

// mergeWithNonTopicPartitions merges the assignments from the fallback balancer with the assignments from the bucket balancer.
func (bpa *PriorityBucketAssigner) mergeWithNonTopicPartitions(membersByNonBucketTopic []GroupMember, partitionsByNonBucketTopic []Partition, groupAssignments GroupMemberAssignments) {
	nonBucketGroupAssignments := bpa.fallback.AssignGroups(membersByNonBucketTopic, partitionsByNonBucketTopic)

	for memberID, assignmentsByNonBucketTopic := range nonBucketGroupAssignments {
		existingGroupAssignmentsByTopic, ok := groupAssignments[memberID]
		if !ok {
			groupAssignments[memberID] = assignmentsByNonBucketTopic
		} else {
			for topic, partitions := range assignmentsByNonBucketTopic {
				existingPartitions := existingGroupAssignmentsByTopic[topic]
				existingGroupAssignmentsByTopic[topic] = append(existingPartitions, filterDuplicates(partitions, existingPartitions)...)
			}
			groupAssignments[memberID] = existingGroupAssignmentsByTopic
		}
		for _, partitions := range assignmentsByNonBucketTopic {
			sort.Ints(partitions)
		}
	}
}

// filterPartitionsByBucketTopic extracts the partitions associated with the topic and non-topic from the
// list of Partitions provided.
func (bpa *PriorityBucketAssigner) filterPartitionsByBucketTopic(partitions []Partition) ([]Partition, []Partition) {
	var partitionsByBucketTopic []Partition
	var partitionsByNonBucketTopic []Partition
	for _, partition := range partitions {
		if partition.Topic() == bpa.config.Topic {
			partitionsByBucketTopic = append(partitionsByBucketTopic, partition)
		} else {
			partitionsByNonBucketTopic = append(partitionsByNonBucketTopic, partition)
		}
	}
	sort.Slice(partitionsByBucketTopic, func(i, j int) bool {
		return partitions[i].ID() < partitions[j].ID()
	})

	return partitionsByBucketTopic, partitionsByNonBucketTopic
}

// filterMembersByBucketTopic groups the memberGroupMetadata by topic.
func (bpa *PriorityBucketAssigner) filterMembersByBucketTopic(members []GroupMember) ([]GroupMember, []GroupMember) {
	var membersByBucketTopic []GroupMember
	var membersByNonBucketTopic []GroupMember
	for _, member := range members {
		for _, topic := range member.Topics() {
			if topic == bpa.config.Topic {
				membersByBucketTopic = append(membersByBucketTopic, member)
			} else {
				membersByNonBucketTopic = append(membersByNonBucketTopic, member)
			}
		}
	}

	sort.Slice(membersByBucketTopic, func(i, j int) bool {
		return members[i].ID() < members[j].ID()
	})

	return membersByBucketTopic, membersByNonBucketTopic
}

// updateBucketPartitions updates the partitions associated with the buckets.
func (bpa *PriorityBucketAssigner) updateBucketPartitions(partitions []Partition) error {
	partitionIDs := make([]int, 0)
	for _, partition := range partitions {
		partitionIDs = append(partitionIDs, partition.ID())
	}
	return updatePartitionsAssignment(bpa.buckets, partitionIDs)
}

// filterDuplicates returns a slice containing the values from both slices provided
func filterDuplicates(slice1 []int, slice2 []int) []int {
	filteredSlice := make([]int, 0, len(slice1)+len(slice2))
	seenValues := make(map[int]bool, len(slice1)+len(slice2))
	for _, value := range slice1 {
		if !seenValues[value] {
			filteredSlice = append(filteredSlice, value)
			seenValues[value] = true
		}
	}
	for _, value := range slice2 {
		if !seenValues[value] {
			filteredSlice = append(filteredSlice, value)
			seenValues[value] = true
		}
	}
	return filteredSlice
}
