package topicservice

import (
	"context"

	"own-kafka/internal/domain"
)

type TopicStorageI interface {
	GetTopicsByName(ctx context.Context, names []string) (map[string]domain.Topic, error)
}

type TopicService struct {
	storage TopicStorageI
}

func New(storage TopicStorageI) *TopicService {
	return &TopicService{
		storage: storage,
	}
}

func (s *TopicService) GetTopicsByNames(ctx context.Context, names []string) (map[string]domain.Topic, error) {
	topics, err := s.storage.GetTopicsByName(ctx, names)
	if err != nil {
		return nil, err
	}

	return topics, nil
}
