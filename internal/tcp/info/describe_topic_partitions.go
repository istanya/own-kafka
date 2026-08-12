package tcpinfo

import (
	"context"
	"fmt"
	"slices"

	"github.com/google/uuid"

	handlersinfo "own-kafka/internal/tcp/handlers/info"
)

func (a infoAPI) DescribeTopicPartitions(ctx context.Context, in *handlersinfo.DescribeTopicPartitionsBodyRequest) (*handlersinfo.DescribeTopicPartitionsBodyResponse, error) {
	names := make([]string, 0, len(in.TopicsArray))
	for _, topic := range in.TopicsArray {
		names = append(names, topic.TopicName)
	}

	topicsMap, err := a.tcpicService.GetTopicsByNames(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("failed to get topics by names: %v", err)
	}

	// The topics in response must be sorted alphabetically by topic name. This ensures consistent ordering regardless of how the client sends the request.
	slices.Sort(names)

	topicsArrayResponse := make([]handlersinfo.TopicResponse, 0, len(in.TopicsArray))
	for _, name := range names {
		topic, ok := topicsMap[name]
		if !ok {
			topicsArrayResponse = append(topicsArrayResponse, handlersinfo.TopicResponse{
				ErrorCode:                 handlersinfo.UnknownTopicErrCode,
				TopicName:                 name,
				TopicId:                   uuid.UUID{},
				IsInternal:                false,
				Partitions:                nil,
				TopicAuthorizedOperations: [4]byte{0x00, 0x00, 0x0d, 0xf8},
				TagBuffer:                 uint8(0),
			})
			continue
		}

		partitions := make([]handlersinfo.Partition, 0, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			partitions = append(partitions, handlersinfo.Partition{
				ErrorCode:              0,
				PartitionIndex:         partition.PartitionID,
				LeaderID:               partition.LeaderID,
				LeaderEpoch:            partition.LeaderEpoch,
				ReplicaNodes:           []uint32{},
				ISRNodes:               []uint32{},
				EligibleLeaderReplicas: []uint32{},
				LastKnownELR:           []uint32{},
				OfflineReplicas:        []uint32{},
				TagBuffer:              uint8(0),
			})
		}

		topicsArrayResponse = append(topicsArrayResponse, handlersinfo.TopicResponse{
			ErrorCode:                 handlersinfo.NoErr,
			TopicName:                 topic.TopicName,
			TopicId:                   topic.TopicUUID,
			IsInternal:                false,
			Partitions:                partitions,
			TopicAuthorizedOperations: [4]byte{0x00, 0x00, 0x0d, 0xf8},
			TagBuffer:                 uint8(0),
		})
	}

	return &handlersinfo.DescribeTopicPartitionsBodyResponse{
		ThrottleTimeMs: 0,
		TopicsArray:    topicsArrayResponse,
		NextCursor:     uint8(255),
		TagBuffer:      uint8(0),
	}, nil
}
