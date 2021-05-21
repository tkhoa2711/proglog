package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tkhoa2711/proglog/api/v1"
	"github.com/tkhoa2711/proglog/internal/log"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log": testProduceConsume,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	config *Config,
	teardown func(),
) {
	t.Helper()

	// Setup the log
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	commitLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Create a listener on local network that our server will run on and
	// our client will connect to
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// Create the test gRPC server
	config = &Config{CommitLog: commitLog}
	server, err := NewGRPCServer(config)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	// Initialize the test client
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	clientConn, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	client = api.NewLogClient(clientConn)

	return client, config, func() {
		server.Stop()
		clientConn.Close()
		l.Close()
		commitLog.Close()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("Hello World!"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}
