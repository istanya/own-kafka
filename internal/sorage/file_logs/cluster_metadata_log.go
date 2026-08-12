package filelogs

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"own-kafka/internal/domain"
	"own-kafka/internal/sorage/file_logs/models"
)

func (s *Storage) getClusterMethadataLog() (*models.ClusterMetadataLogFile, error) {
	file_path := "__cluster_metadata-0/00000000000000000000.log"
	fullPath := filepath.Join(s.storageDirPath, file_path)
	file_, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file: %v", err)
	}
	defer file_.Close()
	file := bufio.NewReader(file_)

	var clusterMetadataLogFile models.ClusterMetadataLogFile
	clusterMetadataLogFile.InitFromFile(file)

	return &clusterMetadataLogFile, nil
}

func (s *Storage) GetTopicsByName(ctx context.Context, names []string) (map[string]domain.Topic, error) {
	clusterMetadataLogFile, err := s.getClusterMethadataLog()
	if err != nil {
		return nil, err
	}

	topicNameMapTopics := make(map[string]*models.TopicRecord, len(names))
	for _, name := range names {
		topicNameMapTopics[name] = nil
	}

	uuidMapPartitions := make(map[[16]byte][]*models.PartitionRecord, len(names))
	for _, butch := range clusterMetadataLogFile.RecordButch {
		for _, record := range butch.Records {
			switch v := record.Value.(type) {
			case *models.TopicRecord:
				_, ok := topicNameMapTopics[v.TopicName]
				if ok {
					topicNameMapTopics[v.TopicName] = v
				}

			case *models.PartitionRecord:
				partitions, ok := uuidMapPartitions[v.TopicUUID]
				if !ok {
					uuidMapPartitions[v.TopicUUID] = []*models.PartitionRecord{v}
				}
				partitions = append(partitions, v)
				uuidMapPartitions[v.TopicUUID] = partitions
			}
		}
	}

	topicsMapName := make(map[string]domain.Topic, len(topicNameMapTopics))
	for name, topic := range topicNameMapTopics {
		if topic == nil {
			continue
		}
		domainPartitions := make([]domain.Partition, 0)
		partitions, ok := uuidMapPartitions[topic.TopicUUID]
		if ok {
			for _, partition := range partitions {
				domainPartitions = append(domainPartitions, domain.Partition{
					PartitionID: partition.PartitionID,
					LeaderID:    partition.Leader,
					LeaderEpoch: partition.LeaderEpoch,
				})
			}
		}
		topicsMapName[name] = domain.Topic{
			TopicName:  name,
			TopicUUID:  topic.TopicUUID,
			Partitions: domainPartitions,
		}
	}
	return topicsMapName, nil
}
