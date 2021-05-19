package log

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tkhoa2711/proglog/api/v1"
)

func makeSegment(baseOffset uint64) (s *segment, dir string, err error) {
	dir, err = ioutil.TempDir("", "segment-test")
	if err != nil {
		return nil, "", err
	}

	c := Config{}
	c.Segment.MaxIndexBytes = entryWidth * 3

	s, err = newSegment(dir, 16, c)
	if err != nil {
		return nil, "", err
	}

	return s, dir, nil
}

func TestNewSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxIndexBytes = entryWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.baseOffset)
	require.Equal(t, uint64(16), s.nextOffset)
}

func TestSegmentAppend(t *testing.T) {
	var baseOffset = uint64(16)
	s, dir, err := makeSegment(baseOffset)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("Hello World!")}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset+i, off)
	}
}

func TestSegmentReadAfterAppend(t *testing.T) {
	var baseOffset = uint64(16)
	s, dir, err := makeSegment(baseOffset)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("Hello World!")}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
}

func TestSegmentClose(t *testing.T) {
	var baseOffset = uint64(16)
	s, dir, err := makeSegment(baseOffset)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	err = s.Close()
	require.NoError(t, err)
}

func TestSegmentRemove(t *testing.T) {
	var baseOffset = uint64(16)
	s, dir, err := makeSegment(baseOffset)
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	err = s.Remove()
	require.NoError(t, err)

	// check that the store and index files had been removed
	if _, err := os.Stat(s.index.Name()); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}
	if _, err := os.Stat(s.store.Name()); !errors.Is(err, os.ErrNotExist) {
		require.NoError(t, err)
	}
}
