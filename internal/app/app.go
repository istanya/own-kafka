package app

import (
	"log/slog"

	tcpapp "own-kafka/internal/app/tcp"
	topicservice "own-kafka/internal/services/topic-service"
	filelogs "own-kafka/internal/sorage/file_logs"
)

type App struct {
	TCPCServer *tcpapp.App
}

func New(
	log *slog.Logger,
	tcpPort int,
	storagePath string,
) (*App, error) {
	storage, err := filelogs.New(storagePath)
	if err != nil {
		return nil, err
	}

	topicService := topicservice.New(storage)

	tcpApp, err := tcpapp.New(log, tcpPort, topicService)

	return &App{
		TCPCServer: tcpApp,
	}, nil
}
