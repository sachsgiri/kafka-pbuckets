package pbuckets

// Partition carries the metadata associated with a kafka partition.
type Partition interface {
	// Topic Name of the topic that the partition belongs to, and its index in the
	// topic.
	Topic() string

	// ID of the partition.
	ID() int
}
