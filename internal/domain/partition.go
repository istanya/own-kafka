package domain

type Partition struct {
	// PartitionIndex is a 4-byte big-endian integer representing the index of this partition.
	PartitionID uint32
	// LeaderID is a 4-byte big-endian integer representing the ID of the leader for this partition.
	LeaderID uint32
	// LeaderEpoch is a 4-byte big-endian integer representing the epoch of the leader.
	LeaderEpoch uint32
}