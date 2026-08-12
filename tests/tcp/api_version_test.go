package tcp

import (
	"encoding/hex"
	"fmt"
	"net"
)


func (s *tcpSuite) TestApiVersion_HappyPath(){
	tests := []struct {
		name string
		request           string
		expectResponse           string
	}{
		{
			name: "all api version",
			request: "000000260012000457e4075b000c6b61666b612d746573746572000a6b61666b612d636c6904302e3100",
			expectResponse: "0000001a57e4075b00000300120000000400004b00000000000000000000",
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
	