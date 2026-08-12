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
) *App {
	storage, err := filelogs.New(storagePath)
	if err != nil {
		panic(err)
	}

	topicService := topicservice.New(storage)

	tcpApp := tcpapp.New(log, tcpPort, topicService)

	return &App{
		TCPCServer: tcpApp,
	}
}
