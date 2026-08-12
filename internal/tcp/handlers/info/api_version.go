package handlersinfo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"own-kafka/internal/tcp/handlers"
)

type ApiVersionResponse struct {
	ApiKey     uint16
	MinVersion uint16
	MaxVersion uint16
	TagBuffer  uint8
}

func (r ApiVersionResponse) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.ApiKey)
	binary.Write(buf, binary.BigEndian, r.MinVersion)
	binary.Write(buf, binary.BigEndian, r.MaxVersion)
	binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

type ApiVersionsBodyResponse struct {
	ErrorCode      uint16
	ArrayLength    uint8
	ApiVersions    []ApiVersionResponse
	ThrottleTimeMs uint32
	TagBuffer      uint8
}

func NewApiVersionsBodyResponse(apiVersion uint16) ApiVersionsBodyResponse {
	var errorCode uint16 = 0
	if apiVersion > 4 {
		errorCode = 35
	}

	return ApiVersionsBodyResponse{
		ErrorCode:   errorCode,
		ArrayLength: 3,
		ApiVersions: []ApiVersionResponse{
			{
				ApiKey:     18,
				MinVersion: 0,
				MaxVersion: 4,
				TagBuffer:  0,
			},
			{
				ApiKey:     75,
				MinVersion: 0,
				MaxVersion: 0,
				TagBuffer:  0,
			},
		},
		ThrottleTimeMs: 0,
		TagBuffer:      0,
	}
}

func (r ApiVersionsBodyResponse) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)
	binary.Write(buf, binary.BigEndian, r.ArrayLength)

	for _, apiVersion := range r.ApiVersions {
		buf.Write(apiVersion.ToBytes())
	}

	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(buf, binary.BigEndian, r.TagBuffer)

	return buf.Bytes()
}

func InfoApiVersionHandler(srv any, ctx context.Context, buf []byte) ([]byte, error) {
	reqBuf := bytes.NewBuffer(buf)
	var kafkaHeaderRequest handlers.KafkaHeaderRequest
	err := kafkaHeaderRequest.Init(reqBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kafka header request: %v", err)
	}

	headerResp := handlers.NewKafkaHeaderResponseV0(kafkaHeaderRequest.CorrelationID)

	resp := NewApiVersionsBodyResponse(kafkaHeaderRequest.ApiVersion)

	out := append(headerResp.ToBytes(), resp.ToBytes()...)

	return out, nil
}
