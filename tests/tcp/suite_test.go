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

	port int
}

func (s *tcpSuite) SetupTest() {
	log := setupLogger()
	s.port = 9001
	application := app.New(log, s.port, "../../tests/storage/kraft-combined-logs")

	go func() {
		err := application.TCPCServer.Run()
		s.NoError(err)
	}()

	time.Sleep(1 * time.Millisecond)
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
