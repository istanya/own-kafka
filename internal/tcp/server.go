package tcpserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"own-kafka/internal/domain"
)

type IBodyResponse interface {
	ToBytes() []byte
}

type IHeaderResponse interface {
	ToBytes() []byte
}

type methodHandler func(srv any, ctx context.Context, buf []byte) ([]byte, error)

// MethodDesc represents an RPC service's method specification.
type MethodDesc struct {
	MethodNum   uint16
	Handler     methodHandler
	ServiceImpl any
}

type TopicServiceI interface {
	GetTopicsByNames(ctx context.Context, names []string) (map[string]domain.Topic, error)
}

type Server struct {
	log     *slog.Logger

	listener net.Listener

	methods map[uint16]*MethodDesc
}

func New(
	log *slog.Logger,
	l net.Listener,
	topicService TopicServiceI,
) *Server {
	return &Server{
		log:     log,
		listener:l,
		methods: make(map[uint16]*MethodDesc),
	}
}

func (s *Server) RegisterService(methods []*MethodDesc) error {
	for _, method := range methods {
		_, ok := s.methods[method.MethodNum]
		if ok {
			return fmt.Errorf("method '%d' already registered", method.MethodNum)
		}
		s.methods[method.MethodNum] = method
	}
	return nil
}

func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %v", err)
		}

		go s.handleClient(conn)
	}
}

func (s *Server) GracefulStop() {
	s.listener.Close()
}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.log.Info("client disconnected")
				return
			}
			s.log.Error("error reading from connection", "error", err)
			return
		}

		resp, err := s.handleReq(buf)
		if err != nil {
			s.log.Error("error handling request", "error", err)
			return
		}

		_, err = conn.Write(resp)
		if err != nil {
			s.log.Error("error writing to connection", "error", err)
			return
		}
	}
}

func (s *Server) handleReq(buf []byte) ([]byte, error) {
	resp, err := s.handle(buf)
	if err != nil {
		return nil, fmt.Errorf("handler.Handle: %s", err.Error())
	}

	return resp, nil
}

func (s *Server) handle(reqBuf []byte) ([]byte, error) {
	ctx := context.Background()
	apiKey := binary.BigEndian.Uint16(reqBuf[4:6])

	method, ok := s.methods[apiKey]
	if !ok {
		return nil, fmt.Errorf("api key '%d' not registered", apiKey)
	}

	resp, err := method.Handler(method.ServiceImpl, ctx, reqBuf)
	if err != nil {
		return nil, fmt.Errorf("handler.Handle: %s", err.Error())
	}

	respBuf := new(bytes.Buffer)
	binary.Write(respBuf, binary.BigEndian, uint32(len(resp)))
	respBuf.Write(resp)

	return respBuf.Bytes(), nil
}
