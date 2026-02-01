package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// OpType represents the type of operation in the WAL.
type OpType uint8

const (
	OpSet OpType = iota
	OpDelete
)

// Record represents a single WAL record.
type Record struct {
	Op        OpType
	ShardID   uint64
	Timestamp int64
	Key       []byte
	Value     []byte
	TTL       int64
	Version   types.Version
	CRC       uint32
}

// WAL represents the write-ahead log.
type WAL struct {
	mu          sync.Mutex
	mode        types.WALMode
	dir         string
	currentFile *os.File
	writer      *bufio.Writer
	fileSize    int64
	maxFileSize int64
	fileSeq     int64
}

// New creates a new WAL instance.
func New(dir string, mode types.WALMode, maxFileSize int64) (*WAL, error) {
	if mode == types.WALModeOff {
		return &WAL{mode: mode}, nil
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &WAL{
		mode:        mode,
		dir:         dir,
		maxFileSize: maxFileSize,
	}

	// Find existing WAL files to determine the highest sequence number
	files, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err == nil && len(files) > 0 {
		var maxSeq int64
		for _, f := range files {
			var seq int64
			base := filepath.Base(f)
			if _, err := fmt.Sscanf(base, "wal-%d.log", &seq); err == nil {
				if seq > maxSeq {
					maxSeq = seq
				}
			}
		}
		w.fileSeq = maxSeq
	}

	if err := w.openNewFile(); err != nil {
		return nil, err
	}

	return w, nil
}

// openNewFile opens a new WAL file.
func (w *WAL) openNewFile() error {
	if w.currentFile != nil {
		w.writer.Flush()
		w.currentFile.Close()
	}

	w.fileSeq++
	filename := filepath.Join(w.dir, fmt.Sprintf("wal-%d.log", w.fileSeq))

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	w.currentFile = f
	w.writer = bufio.NewWriterSize(f, 64*1024)
	w.fileSize = 0

	return nil
}

// Append appends a record to the WAL.
func (w *WAL) Append(rec *Record) error {
	if w.mode == types.WALModeOff {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	rec.Timestamp = time.Now().UnixNano()
	data := w.encodeRecord(rec)

	rec.CRC = crc32.ChecksumIEEE(data[:len(data)-4])
	binary.LittleEndian.PutUint32(data[len(data)-4:], rec.CRC)

	n, err := w.writer.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write WAL record: %w", err)
	}
	w.fileSize += int64(n)

	// For Write mode: flush to kernel buffer (survives process crash, not power loss)
	// For Fsync mode: flush + sync to disk (survives power loss)
	switch w.mode {
	case types.WALModeWrite:
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
	case types.WALModeFsync:
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
		if err := w.currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	if w.fileSize >= w.maxFileSize {
		return w.openNewFile()
	}

	return nil
}

// encodeRecord encodes a record to bytes.
func (w *WAL) encodeRecord(rec *Record) []byte {
	keyLen := len(rec.Key)
	valLen := len(rec.Value)

	size := 1 + 8 + 8 + 4 + keyLen + 4 + valLen + 8 + 8 + 8 + 4
	data := make([]byte, size)

	offset := 0

	data[offset] = byte(rec.Op)
	offset++

	binary.LittleEndian.PutUint64(data[offset:], rec.ShardID)
	offset += 8

	binary.LittleEndian.PutUint64(data[offset:], uint64(rec.Timestamp))
	offset += 8

	binary.LittleEndian.PutUint32(data[offset:], uint32(keyLen))
	offset += 4
	copy(data[offset:], rec.Key)
	offset += keyLen

	binary.LittleEndian.PutUint32(data[offset:], uint32(valLen))
	offset += 4
	copy(data[offset:], rec.Value)
	offset += valLen

	binary.LittleEndian.PutUint64(data[offset:], uint64(rec.TTL))
	offset += 8

	binary.LittleEndian.PutUint64(data[offset:], rec.Version.Term)
	offset += 8
	binary.LittleEndian.PutUint64(data[offset:], rec.Version.Seq)
	offset += 8

	return data
}

// Replay replays all WAL records.
func (w *WAL) Replay(handler func(*Record) error) error {
	if w.mode == types.WALModeOff {
		return nil
	}

	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}

	for _, filename := range files {
		if err := w.replayFile(filename, handler); err != nil {
			return fmt.Errorf("failed to replay %s: %w", filename, err)
		}
	}

	return nil
}

// replayFile replays a single WAL file.
func (w *WAL) replayFile(filename string, handler func(*Record) error) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	for {
		rec, err := w.decodeRecord(reader)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Truncated record at end of file - this happens when a process
			// is killed during a write. The record is incomplete, so we stop
			// replaying this file but don't treat it as an error.
			break
		}
		if err != nil {
			return err
		}

		if err := handler(rec); err != nil {
			return err
		}
	}

	return nil
}

// decodeRecord decodes a record from bytes.
func (w *WAL) decodeRecord(reader *bufio.Reader) (*Record, error) {
	header := make([]byte, 1+8+8)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}

	rec := &Record{}
	offset := 0

	rec.Op = OpType(header[offset])
	offset++

	rec.ShardID = binary.LittleEndian.Uint64(header[offset:])
	offset += 8

	rec.Timestamp = int64(binary.LittleEndian.Uint64(header[offset:]))

	keyLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, keyLenBuf); err != nil {
		return nil, err
	}
	keyLen := binary.LittleEndian.Uint32(keyLenBuf)

	rec.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(reader, rec.Key); err != nil {
		return nil, err
	}

	valLenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, valLenBuf); err != nil {
		return nil, err
	}
	valLen := binary.LittleEndian.Uint32(valLenBuf)

	rec.Value = make([]byte, valLen)
	if _, err := io.ReadFull(reader, rec.Value); err != nil {
		return nil, err
	}

	trailer := make([]byte, 8+8+8+4)
	if _, err := io.ReadFull(reader, trailer); err != nil {
		return nil, err
	}

	rec.TTL = int64(binary.LittleEndian.Uint64(trailer[0:]))
	rec.Version.Term = binary.LittleEndian.Uint64(trailer[8:])
	rec.Version.Seq = binary.LittleEndian.Uint64(trailer[16:])
	rec.CRC = binary.LittleEndian.Uint32(trailer[24:])

	return rec, nil
}

// Flush flushes any buffered data.
func (w *WAL) Flush() error {
	if w.mode == types.WALModeOff {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if w.mode == types.WALModeFsync {
		return w.currentFile.Sync()
	}

	return nil
}

// Close closes the WAL.
func (w *WAL) Close() error {
	if w.mode == types.WALModeOff {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		w.writer.Flush()
	}
	if w.currentFile != nil {
		return w.currentFile.Close()
	}
	return nil
}

// Truncate removes old WAL files up to the given sequence.
func (w *WAL) Truncate(upToSeq int64) error {
	if w.mode == types.WALModeOff {
		return nil
	}

	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return err
	}

	for _, filename := range files {
		var seq int64
		base := filepath.Base(filename)
		if _, err := fmt.Sscanf(base, "wal-%d.log", &seq); err != nil {
			continue
		}

		if seq <= upToSeq && seq < w.fileSeq {
			if err := os.Remove(filename); err != nil {
				return fmt.Errorf("failed to remove old WAL file %s: %w", filename, err)
			}
		}
	}

	return nil
}

// AppendSet appends a SET operation to the WAL.
func (w *WAL) AppendSet(shardID uint64, key, value []byte, ttl int64, version types.Version) error {
	return w.Append(&Record{
		Op:      OpSet,
		ShardID: shardID,
		Key:     key,
		Value:   value,
		TTL:     ttl,
		Version: version,
	})
}

// AppendDelete appends a DELETE operation to the WAL.
func (w *WAL) AppendDelete(shardID uint64, key []byte, version types.Version) error {
	return w.Append(&Record{
		Op:      OpDelete,
		ShardID: shardID,
		Key:     key,
		Version: version,
	})
}

// Mode returns the current WAL mode.
func (w *WAL) Mode() types.WALMode {
	return w.mode
}
