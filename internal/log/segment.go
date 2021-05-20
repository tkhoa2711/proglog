package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/tkhoa2711/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// The segment wraps the index and store types to coordinate operations across
// the two.
//
// The base offset tells us where the segment starts. The next offset allows us
// to know where to append new records.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment creates a new segment in the given directory with the given base
// offset and config.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// Create the store
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Create the index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Determine the next offset to append new records
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record to the segment and returns its offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	b, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(b)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	b, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	if err = proto.Unmarshal(b, record); err != nil {
		return nil, err
	}

	return record, nil
}

// Close closes the segment's store and index files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// Remove closes the segment and remove all of its store and index files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// IsMaxed checks if the segment has reached its size limit, either for the
// store or the index.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}
