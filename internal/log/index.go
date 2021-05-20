package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	offWidth   uint64 = 4
	posWidth   uint64 = 8
	entryWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	// Grow the file to the max index size before memory-mapping it
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close flushes any pending changes and then closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read takes an offset and returns the associated record's position in the
// store. If the input offset is negative, it will read the index in reverse
// direction from the end. For example, Read(-1) returns the last entry of the
// index.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in >= 0 {
		out = uint32(in)
	} else {
		out = uint32((i.size / entryWidth) - uint64(-in))
	}
	pos = uint64(out) * entryWidth
	if i.size < pos+entryWidth {
		return 0, 0, io.EOF
	}

	out = encoding.Uint32(i.mmap[pos : pos+offWidth])
	pos = encoding.Uint64(i.mmap[pos+offWidth : pos+entryWidth])
	return out, pos, nil
}

// Write appends the offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}

	encoding.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	encoding.PutUint64(i.mmap[i.size+offWidth:i.size+entryWidth], pos)
	i.size += uint64(entryWidth)
	return nil
}

// Name returns the name of the physical index file.
func (i *index) Name() string {
	return i.file.Name()
}
