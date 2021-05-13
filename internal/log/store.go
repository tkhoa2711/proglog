package log

import (
	"bufio"
	"os"
)

// A wrapper around physical files to store records in
type store struct {
	*os.File
	buf  *bufio.Writer
	size uint64
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
