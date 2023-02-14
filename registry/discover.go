package discovery

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"10.138.56.184:2379"},
		DialTimeout: 5 * time.Second,
	}
)

func EtcdDial(client *clientv3.Client, service string) (*grpc.ClientConn, error) {
	etcdResolver, err := resolver.NewBuilder(client)
	if err != nil {
		return nil, err
	}
	return grpc.Dial(
		"etcd:///"+service,
		grpc.WithResolvers(etcdResolver),
		grpc.WithBlock(),
	)
}

func Register(service, addr string, stop chan error) error {
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		return fmt.Errorf("create etcd client failed: %v", err)
	}
	defer cli.Close()

	resp, err := cli.Grant(context.Background(), 5)
	if err != nil {
		return fmt.Errorf("create lease fialed: %v", err)
	}

	leaseId := resp.ID
	em, err := endpoints.NewManager(cli, service)
	if err != nil {
		return err
	}
	err = em.AddEndpoint(cli.Ctx(), service+"/"+addr, endpoints.Endpoint{Addr: addr}, clientv3.WithLease(leaseId))
	if err != nil {
		return err
	}
	ch, err := cli.KeepAlive(context.Background(), leaseId)
	if err != nil {
		return fmt.Errorf("set keeplive failed: %v", err)
	}
	log.Printf("[%s] register service success\n", addr)

	for {
		select {
		case err := <-stop:
			if err != nil {
				log.Println(err)
			}
			return err
		case <-cli.Ctx().Done():
			log.Println("service closed")
			return nil
		case _, ok := <-ch:
			if !ok {
				log.Println("keep alive channel closed")
				_, err := cli.Revoke(context.Background(), leaseId)
				return err
			}

		}
	}
}
