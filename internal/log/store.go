package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	encoding = binary.BigEndian
)

const (
	lenWidth = 8
)

// A wrapper around physical files to store records in
type store struct {
	*os.File
	buf  *bufio.Writer
	size uint64
	mu   sync.Mutex
}

func newStore(f *os.File) (*store, error) {
	file, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(file.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store. It returns the number of bytes
// written, the position where the store holds the record in its file, and error
// if any.
func (s *store) Append(b []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// Write the length of the record so that when reading the record, we know
	// how many bytes to read
	if err := binary.Write(s.buf, encoding, uint64(len(b))); err != nil {
		return 0, 0, err
	}

	// Write the record data
	numBytesWritten, err := s.buf.Write(b)
	if err != nil {
		return 0, 0, nil
	}

	numBytesWritten += lenWidth
	s.size += uint64(numBytesWritten)
	return uint64(numBytesWritten), pos, nil
}

// Read returns the record stored at a given position and the error, if any
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make sure we flush all data in the buffer before reading the file
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Find out how many bytes we need in order to fetch the record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Retrieve the record data as bytes
	b := make([]byte, encoding.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads len(b) bytes starting at the offset off from the store's file.
// It returns the number of bytes read and the error, if any.
func (s *store) ReadAt(b []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush any buffered data before the read
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(b, off)
}

// Close closes the file and also persists any buffered data before doing so
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
