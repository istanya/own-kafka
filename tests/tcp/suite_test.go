package tcp

import (
	"bufio"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"own-kafka/internal/app"

	"github.com/stretchr/testify/suite"
)

func TestTCPPSuite(t *testing.T) {
	suite.Run(t, new(tcpSuite))
}

type tcpSuite struct {
	suite.Suite

	application *app.App

	port int
}

func (s *tcpSuite) SetupSuite() {
	log := setupLogger()
	s.port = 9001
	application, err := app.New(log, s.port, "../../tests/storage/kraft-combined-logs")
	s.NoError(err)
	s.application = application

	go func(application_ *app.App) {
		err := application_.TCPCServer.Run()
		s.NoError(err)
	}(application)

	time.Sleep(1 * time.Millisecond)
}


func (s *tcpSuite) TearDownSuite() {
	s.application.TCPCServer.Stop()
}

func setupLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
}

func (s *tcpSuite) readRequest(conn net.Conn) []byte {
	reader := bufio.NewReader(conn)

	lengthBuffer := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBuffer)
	s.NoError(err)

	replyLength := binary.BigEndian.Uint32(lengthBuffer)

	replyBody := make([]byte, replyLength)
	_, err = io.ReadFull(reader, replyBody)

	return append(lengthBuffer, replyBody...)
}
