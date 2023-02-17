package etcd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	hash "github.com/mitchellh/hashstructure"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"groupcache/logger"
	"groupcache/registry"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

var (
	prefix = "/micro/registry/"
)

type etcdRegistry struct {
	client *clientv3.Client

	options registry.Options

	sync.RWMutex

	register map[string]uint64

	leases map[string]clientv3.LeaseID
}

func encode(s *registry.Service) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func decode(ds []byte) *registry.Service {
	var s *registry.Service
	json.Unmarshal(ds, &s)
	return s
}

func nodePath(s, id string) string {
	service := strings.Replace(s, "/", "-", -1)
	node := strings.Replace(id, "/", "-", -1)
	return path.Join(prefix, service, node)
}

func servicePath(s string) string {
	return path.Join(prefix, strings.Replace(s, "/", "-", -1))
}

func (e *etcdRegistry) Init(option ...registry.Option) error {
	return configure(e, option...)
}

func (e *etcdRegistry) Options() registry.Options {
	return e.options
}

func (e *etcdRegistry) Register(service *registry.Service, option ...registry.RegisterOption) error {
	if len(service.Nodes) == 0 {
		return errors.New("Require as least one node")
	}
	var gerr error
	for _, node := range service.Nodes {
		err := e.registerNode(service, node, option...)
		if err != nil {
			gerr = err
		}
	}
	return gerr
}

func (e *etcdRegistry) Deregister(service *registry.Service, opts ...registry.DeregisterOption) error {
	if len(service.Nodes) == 0 {
		return errors.New("Require at least one node")
	}
	for _, node := range service.Nodes {
		e.Lock()
		delete(e.register, service.Name+node.ID)
		delete(e.leases, service.Name+node.ID)
		e.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		defer cancel()

		e.options.Logger.Info("Deregistering", zap.String("name", service.Name), zap.String("node", node.ID))
		_, err := e.client.Delete(ctx, nodePath(service.Name, node.ID))
		if err != nil {
			return err
		}
	}

	return nil

}

func (e *etcdRegistry) GetService(name string, option ...registry.GetOption) ([]*registry.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
	defer cancel()

	rsp, err := e.client.Get(ctx, servicePath(name)+"/", clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return nil, registry.ErrNotFound
	}

	serviceMap := map[string]*registry.Service{}

	for _, n := range rsp.Kvs {
		if sn := decode(n.Value); sn != nil {
			s, ok := serviceMap[sn.Version]
			if !ok {
				s = &registry.Service{
					Name:      sn.Name,
					Version:   sn.Version,
					Metadata:  sn.Metadata,
					Endpoints: sn.Endpoints,
				}
				serviceMap[s.Version] = s
			}
			s.Nodes = append(s.Nodes, sn.Nodes...)
		}

	}

	services := make([]*registry.Service, 0, len(serviceMap))

	for _, service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

func (e *etcdRegistry) ListServices(option ...registry.ListOption) ([]*registry.Service, error) {
	versions := make(map[string]*registry.Service)

	ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
	defer cancel()

	rsp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return []*registry.Service{}, nil
	}

	for _, n := range rsp.Kvs {
		sn := decode(n.Value)
		if sn == nil {
			continue
		}

		v, ok := versions[sn.Name+sn.Version]
		if !ok {
			versions[sn.Name+sn.Version] = sn
			continue
		}
		v.Nodes = append(v.Nodes, sn.Nodes...)
	}

	services := make([]*registry.Service, 0, len(versions))

	for _, service := range services {
		services = append(services, service)
	}
	return services, nil
}

func (e *etcdRegistry) Watch(opts ...registry.WatchOption) (registry.Watcher, error) {
	return newEtcdWatcher(e, e.options.Timeout, opts...)
}

func (e *etcdRegistry) String() string {
	return "etcd"
}

func (e *etcdRegistry) registerNode(s *registry.Service, node *registry.Node, opts ...registry.RegisterOption) error {

	if len(s.Nodes) == 0 {
		return errors.New("Require as least one node")
	}
	e.RLock()
	leaseId, ok := e.leases[s.Name+node.ID]
	e.RUnlock()

	log := e.options.Logger

	if !ok {
		ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
		defer cancel()

		resp, err := e.client.Get(ctx, nodePath(s.Name, node.ID), clientv3.WithSerializable())
		if err != nil {
			return err
		}

		for _, kv := range resp.Kvs {
			if kv.Lease > 0 {
				leaseId = clientv3.LeaseID(kv.Lease)

				srv := decode(kv.Value)
				if srv == nil || len(srv.Nodes) == 0 {
					continue
				}

				h, err := hash.Hash(srv.Nodes[0], nil)
				if err != nil {
					continue
				}
				e.Lock()
				e.leases[s.Name+node.ID] = leaseId
				e.register[s.Name+node.ID] = h
				e.Unlock()
				break
			}
		}
	}

	var leaseNotFound bool

	if leaseId > 0 {
		log.Info("Renewing existing lease for ", zap.String("name", s.Name), zap.Int64("leaseId", int64(leaseId)))
		if _, err := e.client.KeepAliveOnce(context.TODO(), leaseId); err != nil {
			if err != rpctypes.ErrLeaseNotFound {
				return err
			}
			log.Info("Lease not found, ", zap.String("name", s.Name), zap.Int64("leaseId", int64(leaseId)))
			leaseNotFound = true
		}
	}

	h, err := hash.Hash(node, nil)
	if err != nil {
		return err
	}
	e.Lock()
	v, ok := e.register[s.Name+node.ID]
	e.Unlock()

	if ok && v == h && !leaseNotFound {
		log.Info("service node unchanged skipping registration")
		return nil
	}
	service := &registry.Service{
		Name:      s.Name,
		Version:   s.Version,
		Metadata:  s.Metadata,
		Endpoints: s.Endpoints,
		Nodes:     []*registry.Node{node},
	}

	var options registry.RegisterOptions
	for _, o := range opts {
		o(&options)
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.options.Timeout)
	defer cancel()

	var lgr *clientv3.LeaseGrantResponse
	if options.TTL.Seconds() > 0 {
		lgr, err = e.client.Grant(ctx, int64(options.TTL.Seconds()))
		if err != nil {
			return err
		}
	}

	if lgr != nil {
		_, err = e.client.Put(ctx, nodePath(service.Name, node.ID), encode(service), clientv3.WithLease(lgr.ID))
	} else {
		_, err = e.client.Put(ctx, nodePath(service.Name, node.ID), encode(service))
	}

	if err != nil {
		return err
	}

	e.Lock()
	e.register[s.Name+node.ID] = h
	if lgr != nil {
		e.leases[s.Name+node.ID] = lgr.ID
	}
	e.Unlock()

	return nil

}

func NewRegistry(opts ...registry.Option) registry.Registry {
	e := &etcdRegistry{
		options:  registry.Options{},
		register: make(map[string]uint64),
		leases:   make(map[string]clientv3.LeaseID),
	}

	username, password := os.Getenv("ETCD_USERNAME"), os.Getenv("ETCD_PASSWORD")
	if len(username) > 0 && len(password) > 0 {
		opts = append(opts, Auth(username, password))
	}
	address := os.Getenv("MICRO_REGISTRY_ADDRESS")
	if len(address) > 0 {
		opts = append(opts, registry.Addrs(address))
	}
	configure(e, opts...)
	return e
}

func configure(e *etcdRegistry, opts ...registry.Option) error {
	config := clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
	}
	for _, o := range opts {
		o(&e.options)
	}
	if e.options.Timeout == 0 {
		e.options.Timeout = 5 * time.Second
	}
	if e.options.Logger == nil {
		e.options.Logger = logger.DefaultLogger
	}
	config.DialTimeout = e.options.Timeout

	if e.options.Secure || e.options.TLSConfig != nil {
		tlsConfig := e.options.TLSConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}
		config.TLS = tlsConfig
	}
	if e.options.Context != nil {
		u, ok := e.options.Context.Value(authKey{}).(*authCreds)
		if ok {
			config.Username = u.Username
			config.Password = u.Password
		}
		cfg, ok := e.options.Context.Value(logConfigKey{}).(*zap.Config)
		if ok && cfg != nil {
			config.LogConfig = cfg
		}
	}

	var cAddrs []string

	for _, address := range e.options.Addrs {
		if len(address) == 0 {
			continue
		}
		addr, port, err := net.SplitHostPort(address)
		if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
			port = "2379"
			addr = address
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		} else if err == nil {
			cAddrs = append(cAddrs, net.JoinHostPort(addr, port))
		}
	}
	if len(cAddrs) > 0 {
		config.Endpoints = cAddrs
	}
	cli, err := clientv3.New(config)
	if err != nil {
		return err
	}
	e.client = cli
	return nil
}
