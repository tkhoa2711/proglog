package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/tkhoa2711/proglog/api/v1"
	"github.com/tkhoa2711/proglog/internal/auth"
	"github.com/tkhoa2711/proglog/internal/config"
	"github.com/tkhoa2711/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		unauthorizedClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log": testProduceConsume,
		"produce/consume stream to/from the log":    testProduceConsumeStream,
		"consume past log boundary":                 testConsumePastLogBoundary,
		"unauthorized access to produce":            testUnauthorizedClientCantProduce,
		"unauthorized access to consume":            testUnauthorizedClientCantConsume,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, unauthorizedClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, unauthorizedClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	unauthorizedClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	// Create a listener on local network that our server will run on and
	// our client will connect to
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
	) {
		// Configure client's TLS credentials to use our CA as the client's root CA
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)

		// Initialize the test client
		tlsCreds := credentials.NewTLS(clientTLSConfig)
		conn, err := grpc.Dial(
			l.Addr().String(),
			grpc.WithTransportCredentials(tlsCreds),
		)
		require.NoError(t, err)

		client := api.NewLogClient(conn)

		return conn, client
	}

	// Initialize an authorized client
	var clientConn *grpc.ClientConn
	clientConn, client = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	// Initialize an unauthorized client
	var unauthorizedConn *grpc.ClientConn
	unauthorizedConn, unauthorizedClient = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// Configure TLS credentials for our gRPC server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	// Setup the log
	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)
	commitLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Create the test gRPC server
	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  commitLog,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, unauthorizedClient, cfg, func() {
		server.Stop()
		clientConn.Close()
		unauthorizedConn.Close()
		l.Close()
		commitLog.Close()
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
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
	client, _ api.LogClient,
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

func testUnauthorizedClientCantProduce(
	t *testing.T,
	_, unauthorizedClient api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produceResp, err := unauthorizedClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Hello World!"),
			},
		},
	)

	require.Nil(t, produceResp)
	require.Error(t, err)
	require.Equal(t, status.Code(err), codes.PermissionDenied)
}

func testUnauthorizedClientCantConsume(
	t *testing.T,
	_, unauthorizedClient api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	consumeResp, err := unauthorizedClient.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)

	require.Nil(t, consumeResp)
	require.Error(t, err)
	require.Equal(t, status.Code(err), codes.PermissionDenied)
}
