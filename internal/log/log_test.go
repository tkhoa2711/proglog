package log

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "log-test")
	require.NoError(t, err)
	c := Config{}
	_, err = NewLog(dir, c)
	require.NoError(t, err)
}

func TestNewLogSetup(t *testing.T) {

}
