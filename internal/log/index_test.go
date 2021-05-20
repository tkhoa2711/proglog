package log

import (
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	config = Config{
		Segment: struct {
			MaxIndexBytes uint64
			MaxStoreBytes uint64
			InitialOffset uint64
		}{
			MaxIndexBytes: 1024,
		},
	}
	entries = []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
)

// makeNewIndexWithSomeData creates a new index with some test data. It also
// returns the underlying physical file that the caller of this function needs
// to explicitly close it.
func makeNewIndexWithSomeData() (*index, *os.File, error) {
	f, err := ioutil.TempFile(os.TempDir(), "new_index_test_*")
	if err != nil {
		return nil, nil, err
	}

	idx, err := newIndex(f, config)
	if err != nil {
		defer os.Remove(f.Name())
		return nil, nil, err
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		if err != nil {
			defer os.Remove(f.Name())
			return nil, nil, err
		}
	}
	return idx, f, nil
}

func TestIndexNewIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "new_index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = newIndex(f, config)
	require.NoError(t, err)
}

func TestIndexNewIndexRehydrateFromExistingFile(t *testing.T) {
	idx, f, err := makeNewIndexWithSomeData()
	defer os.Remove(f.Name())
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, config)
	require.NoError(t, err)

	off, pos, err := idx.Read(0)
	require.NoError(t, err)
	require.Equal(t, entries[0].Off, off)
	require.Equal(t, entries[0].Pos, pos)
}

func TestIndexClose(t *testing.T) {
	idx, f, err := makeNewIndexWithSomeData()
	defer os.Remove(f.Name())
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)

	fi, err := os.Stat(f.Name())
	require.NoError(t, err)
	require.Equal(t, int64(24), fi.Size())
}

var readNegativeIndexTests = []struct {
	in  int64
	off uint32
	pos uint64
	err error
}{
	{-1, entries[1].Off, entries[1].Pos, nil},
	{-2, entries[0].Off, entries[0].Pos, nil},
	{-3, 0, 0, io.EOF},
}

func TestIndexReadNegativeOffset(t *testing.T) {
	idx, f, err := makeNewIndexWithSomeData()
	defer os.Remove(f.Name())
	require.NoError(t, err)

	for _, tt := range readNegativeIndexTests {
		t.Run(strconv.FormatInt(tt.in, 10), func(t *testing.T) {
			off, pos, err := idx.Read(tt.in)

			if tt.err != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.off, off)
				require.Equal(t, tt.pos, pos)
			}
		})
	}
}

func TestIndexRead(t *testing.T) {
	idx, f, err := makeNewIndexWithSomeData()
	defer os.Remove(f.Name())
	require.NoError(t, err)

	for _, want := range entries {
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}
}

func TestIndexReadPastExistingEntries(t *testing.T) {
	idx, f, err := makeNewIndexWithSomeData()
	defer os.Remove(f.Name())
	require.NoError(t, err)

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
}
