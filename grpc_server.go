package groupcache

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"groupcache/consistenthash"
	pb "groupcache/groupcachepb"
	"groupcache/registry"
	"log"
	"net"
	"strings"
	"sync"
)

func init() {
	grpc.Dial("")
}

type GRPCServer struct {
	addr   string
	stop   chan error
	status bool
	mu     sync.Mutex // guards peers and httpGetters
	peers  *consistenthash.Map
}

func NewGRPCServer(addr string) (*GRPCServer, error) {
	server := new(GRPCServer)
	if addr == "" {
		addr = ""
	}
	server.addr = addr
	return server, nil
}

func (s *GRPCServer) Start() error {
	s.mu.Lock()
	if s.status {
		s.mu.Unlock()
		return fmt.Errorf("server already start")
	}
	s.status = true
	s.stop = make(chan error)

	port := strings.Split(s.addr, ":")[1]
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failded to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGroupCacheServer(grpcServer, s)
	// etcd 注册服务
	go func() {
		node := &registry.Node{
			ID:       "1",
			Address:  s.addr,
			Metadata: nil,
		}
		node.Metadata["registry"] = "etcd"
		node.Metadata["server"] = s.String()
		node.Metadata["transport"] = s.String()
		node.Metadata["protocol"] = "grpc"

		service := &registry.Service{
			Name:    "c1",
			Version: "1.0",
			Nodes:   []*registry.Node{node},
		}
		err = registry.Register(service)
		if err != nil {
			log.Fatalf(err.Error())
		}
		close(s.stop)
		err = listen.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		log.Printf("[%s] Revoke service and close tcp socket ok.", s.addr)
	}()

	s.mu.Unlock()
	if err := grpcServer.Serve(listen); s.status && err != nil {
		return fmt.Errorf("failed to serve:%v", err)
	}
	return nil
}

func (s *GRPCServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	group, key := request.GetGroup(), request.GetKey()
	resp := &pb.GetResponse{}
	if key == "" {
		return resp, fmt.Errorf("key required")
	}
	g := GetGroup(group)
	if g == nil {
		return resp, fmt.Errorf("group not found")
	}
	return resp, nil
}

func (s *GRPCServer) String() string {
	return "grpc-server"
}
