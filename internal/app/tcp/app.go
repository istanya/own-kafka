package tcpapp

import (
	"fmt"
	"log/slog"
	"net"

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
) *App {
	tcpServer := tcpserver.New(log, topicService)

	tcpinfo.Register(tcpServer, topicService)

	return &App{
		log:       log,
		port:      port,
		tcpServer: tcpServer,
	}
}

// Run runs TCP server.
func (a *App) Run() error {
	const op = "tcpapp.Run"

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", a.port))
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	a.log.Info("tcp server started", slog.String("addr", l.Addr().String()))

	if err := a.tcpServer.Serve(l); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// Stop stops TCP server.
func (a *App) Stop() {
	const op = "tcpapp.Stop"

	a.log.With(slog.String("op", op)).
		Info("stopping gRPC server", slog.Int("port", a.port))
}
