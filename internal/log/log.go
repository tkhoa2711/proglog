package log

import (
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/tkhoa2711/proglog/api/v1"
)

type Log struct {
	Dir    string
	Config Config

	segments      []*segment
	activeSegment *segment

	mu sync.RWMutex
}

// NewLog creates a new log based on given config in the given directory.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup initializes the log based on segments that already exists or, if this
// is a new log without existing segments, bootstraps the initial segment.
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		// Files are stored with `offset.[store|index]` format
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsets contains duplicates for index and store files, so we skip
		// the dup here
		// TODO: it's probably better to de-dup the slice separately but this
		//   dead-simple approach works fine for now
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment create a new segment for the log given the base offset
func (l *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Append adds new record to the log and return its offset value.
func (l *Log) Append(record *api.Record) (off uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err = l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read reads the record stored at the given offset.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	i := sort.Search(len(l.segments), func(i int) bool {
		return off < l.segments[i].nextOffset
	})
	if i >= len(l.segments) {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	s = l.segments[i]
	return s.Read(off)
}

// Close closes the log and its segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}
