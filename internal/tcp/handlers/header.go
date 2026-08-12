package handlers

import (
	"bytes"
	"encoding/binary"
	"slices"
)

type KafkaHeaderRequest struct {
	// MessageSize is a 4-byte big-endian integer indicating the size of the rest of the message. 
	MessageSize uint32
	// ApiKey is a 2-byte big-endian integer that identifies the API Key that this request is for.
	ApiKey uint16
	// ApiVersion is a 2-byte big-endian integer indicating the version of the API being used.
	ApiVersion uint16
	// CorrelationID  is a 4-byte big-endian integer that will be echo-ed back in the response. When multiple requests are in-flight, this ID can be used to match responses with their corresponding requests.
	CorrelationID uint32
	// ClientID is a 2-byte big-endian integer indicating the length of the Client ID string.
	ClientIDLength uint16
	// ClientID is a string identifying the client.
	ClientID string
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func (r *KafkaHeaderRequest) Init(buf *bytes.Buffer) error{
	err := binary.Read(buf, binary.BigEndian, &r.MessageSize)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.ApiKey)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.ApiVersion)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.CorrelationID)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.BigEndian, &r.ClientIDLength)
	if err != nil {
		return err
	}

	bufStr := make([]byte, r.ClientIDLength)
	err = binary.Read(buf, binary.BigEndian, &bufStr)
	if err != nil {
		return err
	}
	r.ClientID = string(bufStr)
		
	err = binary.Read(buf, binary.BigEndian, &r.TagBuffer)
	if err != nil {
		return err
	}

	return nil
}

type KafkaHeaderResponseV0 struct {
	// CorrelationID is a 4-byte big-endian integer that matches the ID sent in the corresponding request.
	CorrelationID uint32
}

func NewKafkaHeaderResponseV0(correlationID uint32) KafkaHeaderResponseV0 {
	return KafkaHeaderResponseV0{
		CorrelationID: correlationID,
	}
}

func (r KafkaHeaderResponseV0) ToBytes() []byte {
	bufCorrelationID := make([]byte, 4)
	binary.BigEndian.PutUint32(bufCorrelationID, r.CorrelationID)

	return bufCorrelationID
}

type KafkaHeaderResponseV1 struct {
	// CorrelationID is a 4-byte big-endian integer that matches the ID sent in the corresponding request.
	CorrelationID uint32
	// TagBuffer is tagged fields.
	TagBuffer uint8
}

func NewKafkaHeaderResponseV1(correlationID uint32) KafkaHeaderResponseV1 {
	return KafkaHeaderResponseV1{
		CorrelationID: correlationID,
		TagBuffer:     uint8(0),
	}
}

func (r KafkaHeaderResponseV1) ToBytes() []byte {
	bufCorrelationID := make([]byte, 4)
	binary.BigEndian.PutUint32(bufCorrelationID, r.CorrelationID)

	bufTagBuffer := []byte{r.TagBuffer}

	return slices.Concat(bufCorrelationID, bufTagBuffer)
}