package server

import (
	"context"

	api "github.com/tkhoa2711/proglog/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// NewGRPCServer initializes a new gRPC server with the given config.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	grpcSrv := grpc.NewServer()
	srv := &grpcServer{Config: config}

	api.RegisterLogServer(grpcSrv, srv)
	return grpcSrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error,
) {
	off, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: off}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error,
) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}
