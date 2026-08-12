package tcpinfo

import (
	"context"

	"own-kafka/internal/domain"
	tcpserver "own-kafka/internal/tcp"
	handlersinfo "own-kafka/internal/tcp/handlers/info"
)

type TopicServiceI interface {
	GetTopicsByNames(ctx context.Context, names []string) (map[string]domain.Topic, error)
}

type infoAPI struct {
	tcpicService TopicServiceI
}

func Register(tcpServer *tcpserver.Server, topicService TopicServiceI) {
	infoAPI := infoAPI{tcpicService: topicService}
	InfoServiceDesc := []*tcpserver.MethodDesc{
		{
			MethodNum:   18,
			Handler:     handlersinfo.InfoApiVersionHandler,
			ServiceImpl: nil,
		},
		{
			MethodNum:   75,
			Handler:     handlersinfo.InfoDescribeTopicPartitionsHandler,
			ServiceImpl: infoAPI,
		},
	}

	tcpServer.RegisterService(InfoServiceDesc)
}
