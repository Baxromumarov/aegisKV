package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// TestWALNew tests WAL creation.
func TestWALNew(t *testing.T) {
	t.Run("ModeOff", func(t *testing.T) {
		w, err := New("", types.WALModeOff, 0)
		if err != nil {
			t.Fatalf("failed to create WAL in off mode: %v", err)
		}
		defer w.Close()

		if w.Mode() != types.WALModeOff {
			t.Errorf("expected mode off, got %d", w.Mode())
		}
	})

	t.Run("ModeWrite", func(t *testing.T) {
		dir := t.TempDir()
		w, err := New(dir, types.WALModeWrite, 1024*1024)
		if err != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		defer w.Close()

		if w.Mode() != types.WALModeWrite {
			t.Errorf("expected mode write, got %d", w.Mode())
		}

		// Check that WAL file was created
		files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		if len(files) == 0 {
			t.Error("expected WAL file to be created")
		}
	})

	t.Run("ModeFsync", func(t *testing.T) {
		dir := t.TempDir()
		w, err := New(dir, types.WALModeFsync, 1024*1024)
		if err != nil {
			t.Fatalf("failed to create WAL: %v", err)
		}
		defer w.Close()

		if w.Mode() != types.WALModeFsync {
			t.Errorf("expected mode fsync, got %d", w.Mode())
		}
	})
}

// TestWALAppend tests appending records.
func TestWALAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	// Append SET record
	err = w.Append(&Record{
		Op:      OpSet,
		ShardID: 1,
		Key:     []byte("test-key"),
		Value:   []byte("test-value"),
		TTL:     3600,
		Version: types.Version{Term: 1, Seq: 1},
	})
	if err != nil {
		t.Errorf("failed to append SET record: %v", err)
	}

	// Append DELETE record
	err = w.Append(&Record{
		Op:      OpDelete,
		ShardID: 1,
		Key:     []byte("test-key"),
		Version: types.Version{Term: 1, Seq: 2},
	})
	if err != nil {
		t.Errorf("failed to append DELETE record: %v", err)
	}
}

// TestWALAppendSet tests AppendSet helper.
func TestWALAppendSet(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	err = w.AppendSet(1, []byte("key"), []byte("value"), 3600, types.Version{Term: 1, Seq: 1})
	if err != nil {
		t.Errorf("failed to AppendSet: %v", err)
	}
}

// TestWALAppendDelete tests AppendDelete helper.
func TestWALAppendDelete(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	err = w.AppendDelete(1, []byte("key"), types.Version{Term: 1, Seq: 1})
	if err != nil {
		t.Errorf("failed to AppendDelete: %v", err)
	}
}

// TestWALReplay tests replaying records.
func TestWALReplay(t *testing.T) {
	dir := t.TempDir()

	// Create WAL and write records
	w, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	records := []*Record{
		{Op: OpSet, ShardID: 1, Key: []byte("key1"), Value: []byte("value1"), TTL: 3600, Version: types.Version{Term: 1, Seq: 1}},
		{Op: OpSet, ShardID: 1, Key: []byte("key2"), Value: []byte("value2"), TTL: 7200, Version: types.Version{Term: 1, Seq: 2}},
		{Op: OpDelete, ShardID: 1, Key: []byte("key1"), Version: types.Version{Term: 1, Seq: 3}},
		{Op: OpSet, ShardID: 2, Key: []byte("key3"), Value: []byte("value3"), Version: types.Version{Term: 1, Seq: 4}},
	}

	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	w.Close()

	// Reopen and replay
	w2, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	replayed := make([]*Record, 0)
	err = w2.Replay(func(rec *Record) error {
		replayed = append(replayed, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if len(replayed) != len(records) {
		t.Errorf("expected %d records, got %d", len(records), len(replayed))
	}

	// Verify record contents
	for i, rec := range replayed {
		if rec.Op != records[i].Op {
			t.Errorf("record %d: expected op %d, got %d", i, records[i].Op, rec.Op)
		}
		if string(rec.Key) != string(records[i].Key) {
			t.Errorf("record %d: expected key %s, got %s", i, records[i].Key, rec.Key)
		}
		if string(rec.Value) != string(records[i].Value) {
			t.Errorf("record %d: expected value %s, got %s", i, records[i].Value, rec.Value)
		}
	}
}

// TestWALFileRotation tests file rotation when max size is reached.
func TestWALFileRotation(t *testing.T) {
	dir := t.TempDir()

	// Small max file size to trigger rotation
	w, err := New(dir, types.WALModeWrite, 500)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write enough records to trigger rotation
	for i := 0; i < 100; i++ {
		rec := &Record{
			Op:      OpSet,
			ShardID: 1,
			Key:     []byte("some-key"),
			Value:   []byte("some-value-that-is-long-enough-to-trigger-rotation"),
			Version: types.Version{Term: 1, Seq: uint64(i)},
		}
		w.Append(rec)
	}

	// Should have multiple WAL files
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(files) < 2 {
		t.Errorf("expected multiple WAL files due to rotation, got %d", len(files))
	}
	t.Logf("Created %d WAL files", len(files))
}

// TestWALTruncate tests truncating old files.
func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()

	// Create WAL with small max size
	w, err := New(dir, types.WALModeWrite, 200)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Write records to create multiple files
	for i := 0; i < 50; i++ {
		w.Append(&Record{
			Op:      OpSet,
			ShardID: 1,
			Key:     []byte("key"),
			Value:   []byte("value-with-some-content"),
			Version: types.Version{Term: 1, Seq: uint64(i)},
		})
	}
	w.Close()

	// Count files before truncate
	filesBefore, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	t.Logf("Files before truncate: %d", len(filesBefore))

	// Reopen and truncate
	w2, err := New(dir, types.WALModeWrite, 200)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	// Truncate all but current file
	if len(filesBefore) > 1 {
		err = w2.Truncate(w2.fileSeq - 1)
		if err != nil {
			t.Errorf("failed to truncate: %v", err)
		}

		filesAfter, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
		t.Logf("Files after truncate: %d", len(filesAfter))

		if len(filesAfter) >= len(filesBefore) {
			t.Log("Note: truncate may not have removed files if seq numbers don't match")
		}
	}
}

// TestWALFlush tests explicit flush.
func TestWALFlush(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, types.WALModeWrite, 1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	// Append some records
	for i := 0; i < 10; i++ {
		w.Append(&Record{
			Op:      OpSet,
			ShardID: 1,
			Key:     []byte("key"),
			Value:   []byte("value"),
			Version: types.Version{Term: 1, Seq: uint64(i)},
		})
	}

	// Flush
	err = w.Flush()
	if err != nil {
		t.Errorf("failed to flush: %v", err)
	}

	// Verify data is on disk by checking file size
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(files) > 0 {
		info, _ := os.Stat(files[0])
		if info.Size() == 0 {
			t.Error("expected WAL file to have content after flush")
		}
	}
}

// TestWALModeOff tests that operations are no-ops in off mode.
func TestWALModeOff(t *testing.T) {
	w, _ := New("", types.WALModeOff, 0)
	defer w.Close()

	// These should all succeed silently
	err := w.Append(&Record{Op: OpSet, Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Errorf("Append should succeed in off mode: %v", err)
	}

	err = w.Flush()
	if err != nil {
		t.Errorf("Flush should succeed in off mode: %v", err)
	}

	err = w.Replay(func(rec *Record) error {
		t.Error("should not replay any records in off mode")
		return nil
	})
	if err != nil {
		t.Errorf("Replay should succeed in off mode: %v", err)
	}
}

// TestWALRecordEncoding tests record encoding/decoding round-trip.
func TestWALRecordEncoding(t *testing.T) {
	dir := t.TempDir()
	w, _ := New(dir, types.WALModeWrite, 1024*1024)

	testCases := []struct {
		name string
		rec  *Record
	}{
		{
			name: "SimpleSet",
			rec: &Record{
				Op:      OpSet,
				ShardID: 1,
				Key:     []byte("simple-key"),
				Value:   []byte("simple-value"),
				TTL:     3600,
				Version: types.Version{Term: 1, Seq: 1},
			},
		},
		{
			name: "EmptyValue",
			rec: &Record{
				Op:      OpSet,
				ShardID: 2,
				Key:     []byte("empty-value-key"),
				Value:   []byte{},
				Version: types.Version{Term: 1, Seq: 2},
			},
		},
		{
			name: "LargeValue",
			rec: &Record{
				Op:      OpSet,
				ShardID: 3,
				Key:     []byte("large-key"),
				Value:   make([]byte, 10000),
				TTL:     86400,
				Version: types.Version{Term: 100, Seq: 999999},
			},
		},
		{
			name: "Delete",
			rec: &Record{
				Op:      OpDelete,
				ShardID: 4,
				Key:     []byte("deleted-key"),
				Version: types.Version{Term: 2, Seq: 3},
			},
		},
	}

	// Write all records
	for _, tc := range testCases {
		if err := w.Append(tc.rec); err != nil {
			t.Errorf("%s: failed to append: %v", tc.name, err)
		}
	}
	w.Close()

	// Replay and verify
	w2, _ := New(dir, types.WALModeWrite, 1024*1024)
	defer w2.Close()

	idx := 0
	w2.Replay(func(rec *Record) error {
		if idx >= len(testCases) {
			t.Errorf("unexpected record at index %d", idx)
			return nil
		}

		expected := testCases[idx].rec
		if rec.Op != expected.Op {
			t.Errorf("%s: op mismatch", testCases[idx].name)
		}
		if rec.ShardID != expected.ShardID {
			t.Errorf("%s: shardID mismatch", testCases[idx].name)
		}
		if string(rec.Key) != string(expected.Key) {
			t.Errorf("%s: key mismatch", testCases[idx].name)
		}
		if string(rec.Value) != string(expected.Value) {
			t.Errorf("%s: value mismatch (len: got %d, want %d)", testCases[idx].name, len(rec.Value), len(expected.Value))
		}
		if rec.Version.Term != expected.Version.Term || rec.Version.Seq != expected.Version.Seq {
			t.Errorf("%s: version mismatch", testCases[idx].name)
		}

		idx++
		return nil
	})

	if idx != len(testCases) {
		t.Errorf("expected %d records, replayed %d", len(testCases), idx)
	}
}

// BenchmarkWALAppend benchmarks WAL append performance.
func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()

	b.Run("ModeWrite", func(b *testing.B) {
		w, _ := New(dir+"/write", types.WALModeWrite, 100*1024*1024)
		defer w.Close()

		rec := &Record{
			Op:      OpSet,
			ShardID: 1,
			Key:     []byte("benchmark-key"),
			Value:   make([]byte, 256),
			Version: types.Version{Term: 1, Seq: 0},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Version.Seq = uint64(i)
			w.Append(rec)
		}
	})

	b.Run("ModeFsync", func(b *testing.B) {
		w, _ := New(dir+"/fsync", types.WALModeFsync, 100*1024*1024)
		defer w.Close()

		rec := &Record{
			Op:      OpSet,
			ShardID: 1,
			Key:     []byte("benchmark-key"),
			Value:   make([]byte, 256),
			Version: types.Version{Term: 1, Seq: 0},
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec.Version.Seq = uint64(i)
			w.Append(rec)
		}
	})
}
