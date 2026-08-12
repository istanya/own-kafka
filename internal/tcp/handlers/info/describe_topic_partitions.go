package handlersinfo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"own-kafka/internal/tcp/handlers"

	"github.com/google/uuid"
)

type InfoHandlerI interface {
	// DescribeTopicPartitions
	DescribeTopicPartitions(ctx context.Context, in *DescribeTopicPartitionsBodyRequest) (*DescribeTopicPartitionsBodyResponse, error)
}

type DescribeTopicPartitionsBodyRequest struct {
	// TopicsArrayLength is the length of the topics array + 1, encoded as an unsigned varint.
	TopicsArrayLength uint8
	// TopicsArray is an array of topics to describe.
	TopicsArray []TopicRquest
	// ResponsePartitionLimit is a 4-byte big-endian integer that limits the number of partitions to be returned in the response.
	ResponsePartitionLimit uint32
	// Cursor is a nullable field that can be used for pagination.
	Cursor int8
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r *DescribeTopicPartitionsBodyRequest) Init(buf *bytes.Buffer) error {
	err := binary.Read(buf, binary.BigEndian, &r.TopicsArrayLength)
	if err != nil {
		return err
	}

	arrayLength := r.TopicsArrayLength - 1
	topics := make([]TopicRquest, 0, arrayLength)
	for range arrayLength {
		var topicReq TopicRquest
		err := topicReq.Init(buf)
		if err != nil {
			return err
		}
		topics = append(topics, topicReq)
	}
	r.TopicsArray = topics

	err = binary.Read(buf, binary.BigEndian, &r.ResponsePartitionLimit)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.Cursor)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.TagBuffer)
	if err != nil {
		return err
	}

	return nil
}

// TopicRquest is a single topic in the array.
type TopicRquest struct {
	// TopicNameLength is the length of the topic name + 1, encoded as an unsigned varint.
	TopicNameLength uint8
	// TopicName is the actual topic name encoded in UTF-8.
	TopicName string
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r *TopicRquest) Init(buf *bytes.Buffer) error {
	err := binary.Read(buf, binary.BigEndian, &r.TopicNameLength)
	if err != nil {
		return err
	}

	topicNameLength := r.TopicNameLength - 1
	bufStr := make([]byte, topicNameLength)
	err = binary.Read(buf, binary.BigEndian, &bufStr)
	if err != nil {
		return err
	}
	r.TopicName = string(bufStr)

	err = binary.Read(buf, binary.BigEndian, &r.TagBuffer)
	if err != nil {
		return err
	}

	return nil
}

// TopicResponse is a single topic in the array.
type TopicResponse struct {
	// ErrorCode is a 2-byte big-endian integer representing the error code for this topic.
	ErrorCode ErrCode
	// TopicName Contents is the actual topic name encoded in UTF-8.
	TopicName string
	// TopicId is a 16-byte UUID representing the unique identifier for this topic.
	TopicId uuid.UUID
	// IsInternal is a boolean indicating whether the topic is internal.
	IsInternal bool
	// Partitions is an array of partitions.
	Partitions []Partition
	// TopicAuthorizedOperations is a 4-byte big-endian integer (bitfield) representing the authorized operations for this topic.
	TopicAuthorizedOperations [4]byte
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r TopicResponse) ToBytes() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, r.ErrorCode)

	binary.Write(buf, binary.BigEndian, uint8(len(r.TopicName)+1))
	buf.WriteString(r.TopicName)

	binary.Write(buf, binary.BigEndian, r.TopicId)
	binary.Write(buf, binary.BigEndian, r.IsInternal)

	binary.Write(buf, binary.BigEndian, uint8(len(r.Partitions)+1))
	for _, partition := range r.Partitions {
		buf.Write(partition.ToBytes())
	}

	binary.Write(buf, binary.BigEndian, r.TopicAuthorizedOperations)
	binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

// Partition is a single partition in the array.
type Partition struct {
	// ErrorCode is a 2-byte big-endian integer representing the error code for this partition.
	ErrorCode uint16
	// PartitionIndex is a 4-byte big-endian integer representing the index of this partition.
	PartitionIndex uint32
	// LeaderID is a 4-byte big-endian integer representing the ID of the leader for this partition.
	LeaderID uint32
	// LeaderEpoch is a 4-byte big-endian integer representing the epoch of the leader.
	LeaderEpoch uint32
	// ReplicaNodes is slice of a 4-byte big-endian integer representing a replica node ID.
	ReplicaNodes []uint32
	// ISRNodes is slice of a 4-byte big-endian integer representing an in-sync replica node ID.
	ISRNodes []uint32
	// EligibleLeaderReplicas is an array of eligible leader replica node IDs (int32) for this partition.
	EligibleLeaderReplicas []uint32
	// LastKnownELR is an array of last known eligible leader replica node IDs (int32) for this partition.
	LastKnownELR []uint32
	// OfflineReplicas is an array of offline replica node IDs (int32) for this partition.
	OfflineReplicas []uint32
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r Partition) ToBytes() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	binary.Write(buf, binary.BigEndian, r.PartitionIndex)
	binary.Write(buf, binary.BigEndian, r.LeaderID)
	binary.Write(buf, binary.BigEndian, r.LeaderEpoch)

	binary.Write(buf, binary.BigEndian, uint8(len(r.ReplicaNodes)+1))
	binary.Write(buf, binary.BigEndian, r.ReplicaNodes)

	binary.Write(buf, binary.BigEndian, uint8(len(r.ISRNodes)+1))
	binary.Write(buf, binary.BigEndian, r.ISRNodes)

	binary.Write(buf, binary.BigEndian, uint8(len(r.EligibleLeaderReplicas)+1))
	binary.Write(buf, binary.BigEndian, r.EligibleLeaderReplicas)

	binary.Write(buf, binary.BigEndian, uint8(len(r.LastKnownELR)+1))
	binary.Write(buf, binary.BigEndian, r.LastKnownELR)

	binary.Write(buf, binary.BigEndian, uint8(len(r.OfflineReplicas)+1))
	binary.Write(buf, binary.BigEndian, r.OfflineReplicas)

	binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

type DescribeTopicPartitionsBodyResponse struct {
	// ThrottleTimeMs is a 4-byte big-endian integer that represents the duration in milliseconds for which the request was throttled due to quota violation.
	ThrottleTimeMs uint32
	// TopicsArray is an array of topics.
	TopicsArray []TopicResponse
	// NextCursor is a nullable field that can be used for pagination.
	NextCursor uint8
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r DescribeTopicPartitionsBodyResponse) ToBytes() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)

	binary.Write(buf, binary.BigEndian, uint8(len(r.TopicsArray)+1))
	for _, topic := range r.TopicsArray {
		buf.Write(topic.ToBytes())
	}

	binary.Write(buf, binary.BigEndian, r.NextCursor)
	binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

func InfoDescribeTopicPartitionsHandler(srv any, ctx context.Context, buf []byte) ([]byte, error) {
	reqBuf := bytes.NewBuffer(buf)
	var kafkaHeaderRequest handlers.KafkaHeaderRequest
	err := kafkaHeaderRequest.Init(reqBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kafka header request: %v", err)
	}
	headerResp := handlers.NewKafkaHeaderResponseV1(kafkaHeaderRequest.CorrelationID)

	var in DescribeTopicPartitionsBodyRequest
	err = in.Init(reqBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse describe topic partitions body request: %v", err)
	}

	resp, err := srv.(InfoHandlerI).DescribeTopicPartitions(ctx, &in)
	if err != nil {
		return nil, err
	}

	out := append(headerResp.ToBytes(), resp.ToBytes()...)

	return out, nil
}
