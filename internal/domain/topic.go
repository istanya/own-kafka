package domain

type Topic struct {
	// TopicName is the name of the topic
	TopicName string
	// TopicUUID is the UUID of the topic.
	TopicUUID [16]byte
	// Partitions is an array of partitions.
	Partitions []Partition
}