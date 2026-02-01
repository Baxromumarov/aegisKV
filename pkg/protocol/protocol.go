package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Command types for the binary protocol.
type CommandType uint8

const (
	CmdGet CommandType = iota + 1
	CmdSet
	CmdDel
	CmdPing
	CmdPong
	CmdReplicate
	CmdReplicateAck
	CmdRedirect
	CmdSync
	CmdSyncAck
	CmdStats
	CmdAuth // 12 - Authentication handshake
)

// Response status codes.
type Status uint8

const (
	StatusOK Status = iota
	StatusNotFound
	StatusError
	StatusRedirect
	StatusVersionConflict
)

// Magic bytes for protocol identification.
const (
	MagicByte   byte   = 0xAE
	ProtocolV1  byte   = 0x01
	MaxKeyLen   uint32 = 64 * 1024        // 64KB
	MaxValueLen uint32 = 64 * 1024 * 1024 // 64MB
)

// Request represents a protocol request.
type Request struct {
	Command   CommandType
	Key       []byte
	Value     []byte
	TTL       int64 // milliseconds
	RequestID uint64
	Version   uint64
}

// Response represents a protocol response.
type Response struct {
	Status    Status
	Key       []byte
	Value     []byte
	TTL       int64
	RequestID uint64
	Version   uint64
	NodeAddr  string // for redirects
	Error     string
}

// Encoder encodes protocol messages with reusable buffers.
type Encoder struct {
	w   io.Writer
	buf []byte // reusable buffer, grown as needed
}

// NewEncoder creates a new encoder.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, buf: make([]byte, 0, 4096)}
}

// EncodeRequest encodes a request to the wire format.
func (e *Encoder) EncodeRequest(req *Request) error {
	keyLen := uint32(len(req.Key))
	valLen := uint32(len(req.Value))

	// Header: magic(1) + version(1) + command(1) + requestID(8) + keyLen(4) + valLen(4) + ttl(8) + version(8)
	headerSize := 1 + 1 + 1 + 8 + 4 + 4 + 8 + 8
	totalSize := headerSize + int(keyLen) + int(valLen)

	// Grow reusable buffer if needed (4 byte length prefix + payload)
	needed := 4 + totalSize
	if cap(e.buf) < needed {
		e.buf = make([]byte, needed)
	} else {
		e.buf = e.buf[:needed]
	}

	// Length prefix
	binary.BigEndian.PutUint32(e.buf[0:4], uint32(totalSize))

	buf := e.buf[4:]
	offset := 0

	buf[offset] = MagicByte
	offset++
	buf[offset] = ProtocolV1
	offset++
	buf[offset] = byte(req.Command)
	offset++

	binary.BigEndian.PutUint64(buf[offset:], req.RequestID)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], keyLen)
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:], valLen)
	offset += 4

	binary.BigEndian.PutUint64(buf[offset:], uint64(req.TTL))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], req.Version)
	offset += 8

	copy(buf[offset:], req.Key)
	offset += int(keyLen)

	copy(buf[offset:], req.Value)

	_, err := e.w.Write(e.buf[:needed])
	return err
}

// EncodeResponse encodes a response to the wire format.
func (e *Encoder) EncodeResponse(resp *Response) error {
	keyLen := uint32(len(resp.Key))
	valLen := uint32(len(resp.Value))
	errLen := uint32(len(resp.Error))
	addrLen := uint32(len(resp.NodeAddr))

	// Header: magic(1) + version(1) + status(1) + requestID(8) + keyLen(4) + valLen(4) + ttl(8) + version(8) + errLen(4) + addrLen(4)
	headerSize := 1 + 1 + 1 + 8 + 4 + 4 + 8 + 8 + 4 + 4
	totalSize := headerSize + int(keyLen) + int(valLen) + int(errLen) + int(addrLen)

	needed := 4 + totalSize
	if cap(e.buf) < needed {
		e.buf = make([]byte, needed)
	} else {
		e.buf = e.buf[:needed]
	}

	binary.BigEndian.PutUint32(e.buf[0:4], uint32(totalSize))

	buf := e.buf[4:]
	offset := 0

	buf[offset] = MagicByte
	offset++
	buf[offset] = ProtocolV1
	offset++
	buf[offset] = byte(resp.Status)
	offset++

	binary.BigEndian.PutUint64(buf[offset:], resp.RequestID)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], keyLen)
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:], valLen)
	offset += 4

	binary.BigEndian.PutUint64(buf[offset:], uint64(resp.TTL))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], resp.Version)
	offset += 8

	binary.BigEndian.PutUint32(buf[offset:], errLen)
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:], addrLen)
	offset += 4

	copy(buf[offset:], resp.Key)
	offset += int(keyLen)

	copy(buf[offset:], resp.Value)
	offset += int(valLen)

	copy(buf[offset:], resp.Error)
	offset += int(errLen)

	copy(buf[offset:], resp.NodeAddr)

	_, err := e.w.Write(e.buf[:needed])
	return err
}

// Decoder decodes protocol messages.
type Decoder struct {
	r      io.Reader
	lenBuf [4]byte // fixed-size, avoids allocation per decode
	msgBuf []byte  // reusable message buffer
}

// NewDecoder creates a new decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:      r,
		msgBuf: make([]byte, 0, 4096), // Pre-allocate common size
	}
}

// DecodeRequest decodes a request from the wire format.
// Reuses internal buffer to minimize allocations.
func (d *Decoder) DecodeRequest() (*Request, error) {
	if _, err := io.ReadFull(d.r, d.lenBuf[:]); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(d.lenBuf[:])

	if totalLen > MaxValueLen+MaxKeyLen+100 {
		return nil, errors.New("message too large")
	}

	// Grow reusable buffer if needed
	if cap(d.msgBuf) < int(totalLen) {
		d.msgBuf = make([]byte, totalLen)
	} else {
		d.msgBuf = d.msgBuf[:totalLen]
	}
	buf := d.msgBuf

	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}

	offset := 0

	if buf[offset] != MagicByte {
		return nil, errors.New("invalid magic byte")
	}
	offset++

	if buf[offset] != ProtocolV1 {
		return nil, fmt.Errorf("unsupported protocol version: %d", buf[offset])
	}
	offset++

	req := &Request{
		Command: CommandType(buf[offset]),
	}
	offset++

	req.RequestID = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	keyLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	valLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	req.TTL = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	req.Version = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	if keyLen > MaxKeyLen {
		return nil, errors.New("key too large")
	}
	if valLen > MaxValueLen {
		return nil, errors.New("value too large")
	}

	// Allocate key and value only (these need to outlive the buffer)
	req.Key = make([]byte, keyLen)
	copy(req.Key, buf[offset:offset+int(keyLen)])
	offset += int(keyLen)

	req.Value = make([]byte, valLen)
	copy(req.Value, buf[offset:offset+int(valLen)])

	return req, nil
}

// DecodeResponse decodes a response from the wire format.
func (d *Decoder) DecodeResponse() (*Response, error) {
	if _, err := io.ReadFull(d.r, d.lenBuf[:]); err != nil {
		return nil, err
	}
	totalLen := binary.BigEndian.Uint32(d.lenBuf[:])

	if totalLen > MaxValueLen+MaxKeyLen+1000 {
		return nil, errors.New("message too large")
	}

	buf := make([]byte, totalLen)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}

	offset := 0

	if buf[offset] != MagicByte {
		return nil, errors.New("invalid magic byte")
	}
	offset++

	if buf[offset] != ProtocolV1 {
		return nil, fmt.Errorf("unsupported protocol version: %d", buf[offset])
	}
	offset++

	resp := &Response{
		Status: Status(buf[offset]),
	}
	offset++

	resp.RequestID = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	keyLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	valLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	resp.TTL = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	resp.Version = binary.BigEndian.Uint64(buf[offset:])
	offset += 8

	errLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	addrLen := binary.BigEndian.Uint32(buf[offset:])
	offset += 4

	resp.Key = make([]byte, keyLen)
	copy(resp.Key, buf[offset:offset+int(keyLen)])
	offset += int(keyLen)

	resp.Value = make([]byte, valLen)
	copy(resp.Value, buf[offset:offset+int(valLen)])
	offset += int(valLen)

	if errLen > 0 {
		resp.Error = string(buf[offset : offset+int(errLen)])
		offset += int(errLen)
	}

	if addrLen > 0 {
		resp.NodeAddr = string(buf[offset : offset+int(addrLen)])
	}

	return resp, nil
}

// CommandName returns the name of a command.
func CommandName(cmd CommandType) string {
	switch cmd {
	case CmdGet:
		return "GET"
	case CmdSet:
		return "SET"
	case CmdDel:
		return "DEL"
	case CmdPing:
		return "PING"
	case CmdPong:
		return "PONG"
	case CmdReplicate:
		return "REPLICATE"
	case CmdReplicateAck:
		return "REPLICATE_ACK"
	case CmdRedirect:
		return "REDIRECT"
	case CmdSync:
		return "SYNC"
	case CmdSyncAck:
		return "SYNC_ACK"
	case CmdStats:
		return "STATS"
	case CmdAuth:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

// StatusName returns the name of a status.
func StatusName(status Status) string {
	switch status {
	case StatusOK:
		return "OK"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusError:
		return "ERROR"
	case StatusRedirect:
		return "REDIRECT"
	case StatusVersionConflict:
		return "VERSION_CONFLICT"
	default:
		return "UNKNOWN"
	}
}
