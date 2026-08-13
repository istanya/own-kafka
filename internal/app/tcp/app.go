package tcpapp

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"

	topicservice "own-kafka/internal/services/topic-service"
	tcpserver "own-kafka/internal/tcp"
	tcpinfo "own-kafka/internal/tcp/info"
)

type App struct {
	log       *slog.Logger
	tcpServer *tcpserver.Server
	port      int
}

func New(
	log *slog.Logger,
	port int,
	topicService *topicservice.TopicService,
) (*App,error) {
	const op = "tcpapp.New"

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	tcpServer := tcpserver.New(log, l, topicService)

	tcpinfo.Register(tcpServer, topicService)

	return &App{
		log:       log,
		port:      port,
		tcpServer: tcpServer,
	}, nil
}

// Run runs TCP server.
func (a *App) Run() error {
	const op = "tcpapp.Run"

	if err := a.tcpServer.Serve(); err != nil {
		a.log.Error("failed to start TCP server", slog.String("op", op), slog.Int("port", a.port), slog.Any("error", err))

		return fmt.Errorf("%s: %w", op, err)
	}

	a.log.Info("tcp server started", slog.String("port", strconv.Itoa(a.port)))

	return nil
}

// Stop stops TCP server.
func (a *App) Stop() {
	const op = "tcpapp.Stop"

	a.tcpServer.GracefulStop()

	a.log.With(slog.String("op", op)).
		Info("stopping gRPC server", slog.Int("port", a.port))
}
