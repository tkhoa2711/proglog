package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	recordData  = []byte("hellow world")
	recordWidth = uint64(len(recordData)) + lenWidth
)

func TestAppend(t *testing.T) {
	// TODO: can we extract store creation logic into its own function?
	f, err := os.CreateTemp("", "store_append_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
}

func TestAppendThenRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	// write 4 records with the same data to the store
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(recordData)
		require.NoError(t, err)
		require.Equal(t, pos+n, recordWidth*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		got, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, recordData, got)
		pos += recordWidth
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// Read the length of the record
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)

		require.NoError(t, err)
		require.Equal(t, int(lenWidth), n)

		off += int64(n)

		// Read the record itself
		size := encoding.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)

		require.NoError(t, err)
		require.Equal(t, recordData, b)
		require.Equal(t, int(size), n)

		off += int64(n)
	}
}

func TestClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	_, _, err = s.Append(recordData)
	require.NoError(t, err)

	f, beforeCloseSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterCloseSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterCloseSize > beforeCloseSize)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, fi.Size(), nil
}
