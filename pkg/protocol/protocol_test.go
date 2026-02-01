package protocol

import (
	"bytes"
	"testing"
)

// TestCommandTypes tests command type constants.
func TestCommandTypes(t *testing.T) {
	// Verify command types are distinct and non-zero
	cmds := []CommandType{
		CmdGet,
		CmdSet,
		CmdDel,
		CmdPing,
		CmdPong,
		CmdReplicate,
		CmdReplicateAck,
		CmdRedirect,
		CmdSync,
		CmdSyncAck,
		CmdStats,
		CmdAuth,
	}
	seen := make(map[CommandType]bool)
	for _, cmd := range cmds {
		if cmd == 0 {
			t.Error("command type should not be zero")
		}
		if seen[cmd] {
			t.Errorf("duplicate command type: %d", cmd)
		}
		seen[cmd] = true
	}
}

// TestStatusCodes tests status code constants.
func TestStatusCodes(t *testing.T) {
	if StatusOK != 0 {
		t.Errorf("expected StatusOK to be 0, got %d", StatusOK)
	}
	if StatusNotFound != 1 {
		t.Errorf("expected StatusNotFound to be 1, got %d", StatusNotFound)
	}
}

// TestEncoderDecoder tests request encoding/decoding round-trip.
func TestEncoderDecoderRequest(t *testing.T) {
	testCases := []struct {
		name string
		req  *Request
	}{
		{
			name: "SimpleGet",
			req: &Request{
				Command:   CmdGet,
				Key:       []byte("test-key"),
				RequestID: 12345,
			},
		},
		{
			name: "SetWithValue",
			req: &Request{
				Command:   CmdSet,
				Key:       []byte("key"),
				Value:     []byte("value"),
				TTL:       3600000,
				RequestID: 99999,
				Version:   1,
			},
		},
		{
			name: "EmptyKey",
			req: &Request{
				Command:   CmdPing,
				Key:       []byte{},
				RequestID: 1,
			},
		},
		{
			name: "LargeValue",
			req: &Request{
				Command:   CmdSet,
				Key:       []byte("large-key"),
				Value:     make([]byte, 10000),
				RequestID: 42,
			},
		},
		{
			name: "Delete",
			req: &Request{
				Command:   CmdDel,
				Key:       []byte("delete-me"),
				RequestID: 100,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			encoder := NewEncoder(buf)

			if err := encoder.EncodeRequest(tc.req); err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			decoder := NewDecoder(buf)
			decoded, err := decoder.DecodeRequest()
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if decoded.Command != tc.req.Command {
				t.Errorf("command mismatch: got %d, want %d", decoded.Command, tc.req.Command)
			}
			if !bytes.Equal(decoded.Key, tc.req.Key) {
				t.Errorf("key mismatch")
			}
			if !bytes.Equal(decoded.Value, tc.req.Value) {
				t.Errorf("value mismatch")
			}
			if decoded.TTL != tc.req.TTL {
				t.Errorf("TTL mismatch: got %d, want %d", decoded.TTL, tc.req.TTL)
			}
			if decoded.RequestID != tc.req.RequestID {
				t.Errorf("requestID mismatch: got %d, want %d", decoded.RequestID, tc.req.RequestID)
			}
		})
	}
}

// TestEncoderDecoderResponse tests response encoding/decoding round-trip.
func TestEncoderDecoderResponse(t *testing.T) {
	testCases := []struct {
		name string
		resp *Response
	}{
		{
			name: "OKResponse",
			resp: &Response{
				Status:    StatusOK,
				Key:       []byte("key"),
				Value:     []byte("value"),
				RequestID: 123,
				Version:   5,
			},
		},
		{
			name: "NotFoundResponse",
			resp: &Response{
				Status:    StatusNotFound,
				Key:       []byte("missing"),
				RequestID: 456,
			},
		},
		{
			name: "ErrorResponse",
			resp: &Response{
				Status:    StatusError,
				Error:     "something went wrong",
				RequestID: 789,
			},
		},
		{
			name: "RedirectResponse",
			resp: &Response{
				Status:    StatusRedirect,
				NodeAddr:  "192.168.1.100:7700",
				RequestID: 111,
			},
		},
		{
			name: "WithTTL",
			resp: &Response{
				Status:    StatusOK,
				Value:     []byte("data"),
				TTL:       3600000,
				RequestID: 222,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			encoder := NewEncoder(buf)

			if err := encoder.EncodeResponse(tc.resp); err != nil {
				t.Fatalf("encode failed: %v", err)
			}

			decoder := NewDecoder(buf)
			decoded, err := decoder.DecodeResponse()
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if decoded.Status != tc.resp.Status {
				t.Errorf("status mismatch: got %d, want %d", decoded.Status, tc.resp.Status)
			}
			if !bytes.Equal(decoded.Key, tc.resp.Key) {
				t.Errorf("key mismatch")
			}
			if !bytes.Equal(decoded.Value, tc.resp.Value) {
				t.Errorf("value mismatch")
			}
			if decoded.TTL != tc.resp.TTL {
				t.Errorf("TTL mismatch")
			}
			if decoded.Error != tc.resp.Error {
				t.Errorf("error mismatch: got %q, want %q", decoded.Error, tc.resp.Error)
			}
			if decoded.NodeAddr != tc.resp.NodeAddr {
				t.Errorf("nodeAddr mismatch: got %q, want %q", decoded.NodeAddr, tc.resp.NodeAddr)
			}
		})
	}
}

// TestEncoderBufferReuse tests that encoder reuses buffers.
func TestEncoderBufferReuse(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder := NewEncoder(buf)

	req := &Request{
		Command:   CmdGet,
		Key:       []byte("key"),
		RequestID: 1,
	}

	// Encode multiple times
	for i := 0; i < 100; i++ {
		buf.Reset()
		req.RequestID = uint64(i)
		if err := encoder.EncodeRequest(req); err != nil {
			t.Fatalf("encode %d failed: %v", i, err)
		}
	}

	// Should not have allocated much
	// (This is a basic check - real allocation check would use benchmarks)
}

// TestDecoderInvalidMagic tests handling of invalid magic byte.
func TestDecoderInvalidMagic(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x10, 0xFF, 0x01, 0x01} // Wrong magic byte
	buf := bytes.NewReader(data)
	decoder := NewDecoder(buf)

	_, err := decoder.DecodeRequest()
	if err == nil {
		t.Error("expected error for invalid magic byte")
	}
}

// TestDecoderInvalidVersion tests handling of invalid protocol version.
func TestDecoderInvalidVersion(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x10, MagicByte, 0xFF, 0x01} // Wrong version
	buf := bytes.NewReader(data)
	decoder := NewDecoder(buf)

	_, err := decoder.DecodeRequest()
	if err == nil {
		t.Error("expected error for invalid version")
	}
}

// TestMultipleMessagesOnStream tests multiple messages on same connection.
func TestMultipleMessagesOnStream(t *testing.T) {
	buf := &bytes.Buffer{}
	encoder := NewEncoder(buf)

	// Encode multiple requests
	requests := []*Request{
		{Command: CmdGet, Key: []byte("key1"), RequestID: 1},
		{Command: CmdSet, Key: []byte("key2"), Value: []byte("val2"), RequestID: 2},
		{Command: CmdDel, Key: []byte("key3"), RequestID: 3},
	}

	for _, req := range requests {
		if err := encoder.EncodeRequest(req); err != nil {
			t.Fatalf("encode failed: %v", err)
		}
	}

	// Decode all
	decoder := NewDecoder(buf)
	for i, expected := range requests {
		decoded, err := decoder.DecodeRequest()
		if err != nil {
			t.Fatalf("decode %d failed: %v", i, err)
		}
		if decoded.Command != expected.Command {
			t.Errorf("request %d: command mismatch", i)
		}
		if decoded.RequestID != expected.RequestID {
			t.Errorf("request %d: requestID mismatch", i)
		}
	}
}

// BenchmarkEncodeRequest benchmarks request encoding.
func BenchmarkEncodeRequest(b *testing.B) {
	buf := &bytes.Buffer{}
	encoder := NewEncoder(buf)

	req := &Request{
		Command:   CmdSet,
		Key:       []byte("benchmark-key"),
		Value:     make([]byte, 256),
		TTL:       3600000,
		RequestID: 0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		req.RequestID = uint64(i)
		encoder.EncodeRequest(req)
	}
}

// BenchmarkDecodeRequest benchmarks request decoding.
func BenchmarkDecodeRequest(b *testing.B) {
	// Pre-encode a request
	var encoded bytes.Buffer
	encoder := NewEncoder(&encoded)
	req := &Request{
		Command:   CmdSet,
		Key:       []byte("benchmark-key"),
		Value:     make([]byte, 256),
		TTL:       3600000,
		RequestID: 12345,
	}
	encoder.EncodeRequest(req)
	data := encoded.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder := NewDecoder(bytes.NewReader(data))
		decoder.DecodeRequest()
	}
}
