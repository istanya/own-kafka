package tcp

import (
	"encoding/hex"
	"fmt"
	"net"
)


func (s *tcpSuite) TestDescribeTopicPartitions_HappyPath(){
	tests := []struct {
		name string
		request           string
		expectResponse           string
	}{
		{
			name: "single partition",
			request: "00000020004b00000000000700096b61666b612d636c69000204666f6f0000000064ff00",
			expectResponse: "0000005100000007000000000002000004666f6f0000000000004000800000000000009100030000000000000000000100000000010101010100000000000001000000010000000001010101010000000df800ff00",
		},
		{
			name: "unknown topic",
			request: "0000002f004b0000626aee08000c6b61666b612d746573746572000210554e4b4e4f574e5f544f5049435f360000000001ff00",
			expectResponse: "00000035626aee08000000000002000310554e4b4e4f574e5f544f5049435f3600000000000000000000000000000000000100000df800ff00",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			conn, err := net.Dial("tcp", fmt.Sprintf(":%d", s.port))
			s.NoError(err)

			defer conn.Close()


			bytes, err := hex.DecodeString(tt.request)
			s.NoError(err)

			_, err = conn.Write([]byte(bytes))
			s.NoError(err)

			req := s.readRequest(conn)

			expectBytes, err := hex.DecodeString(tt.expectResponse)
			s.Equal(expectBytes, req)
		})
	}
}
	