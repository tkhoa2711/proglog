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
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log": testProduceConsume,
		"produce/consume stream to/from the log":    testProduceConsumeStream,
		"consume past log boundary":                 testConsumePastLogBoundary,
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

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testConsumePastLogBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	record := &api.Record{
		Value: []byte("Hello World!"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: record},
	)
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset + 1},
	)
	require.Nil(t, consume)

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}
