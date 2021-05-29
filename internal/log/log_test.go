package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tkhoa2711/proglog/api/v1"
)

func fillLogWithData(t *testing.T, log *Log, record *api.Record, len uint) {
	t.Helper()
	for i := uint64(0); i < uint64(len); i++ {
		off, err := log.Append(record)
		require.NoError(t, err)
		require.Equal(t, i, off)
	}
}

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"new empty log":               testNewEmptyLog,
		"new log from existing state": testNewLogFromExistingState,
		"append":                      testLogAppend,
		"append over size limit":      testLogAppendOverSegmentSizeLimit,
		"read 1 segment":              testLogReadOneSegment,
		"read 3 segments":             testLogReadThreeSegments,
		"read out of range":           testLogReadOutOfRange,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxIndexBytes = entryWidth * 3
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testNewEmptyLog(t *testing.T, log *Log) {
}

func testNewLogFromExistingState(t *testing.T, log *Log) {
	// Populate some data
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 3)

	// Close the log to flush all data to disk
	err := log.Close()
	require.NoError(t, err)

	newLog, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)
	require.Equal(t, log.activeSegment.baseOffset, newLog.activeSegment.baseOffset)
	require.Equal(t, log.activeSegment.nextOffset, newLog.activeSegment.nextOffset)
}

func testLogAppend(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 7)
}

func testLogAppendOverSegmentSizeLimit(t *testing.T, log *Log) {
	require.Equal(t, 1, len(log.segments))
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 3)
	require.Equal(t, 2, len(log.segments))
	require.Equal(t, log.segments[1], log.activeSegment)
	require.Equal(t, uint64(3), log.activeSegment.baseOffset)
	require.Equal(t, uint64(3), log.activeSegment.nextOffset)
}

func testLogReadOneSegment(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 3)

	for off := uint64(0); off < 3; off++ {
		got, err := log.Read(off)
		require.NoError(t, err)
		require.Equal(t, record.Value, got.Value)
	}
}

func testLogReadThreeSegments(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 7)

	for off := uint64(0); off < 7; off++ {
		got, err := log.Read(off)
		require.NoError(t, err)
		require.Equal(t, record.Value, got.Value)
	}
}

func testLogReadOutOfRange(t *testing.T, log *Log) {
	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	fillLogWithData(t, log, record, 7)

	got, err := log.Read(8)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(8), apiErr.Offset)
	require.Nil(t, got)
}
