package etcd

import (
	"context"
	"errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"groupcache/registry"
	"time"
)

type etcdWatcher struct {
	stop    chan bool
	w       clientv3.WatchChan
	client  *clientv3.Client
	timeout time.Duration
}

func newEtcdWatcher(e *etcdRegistry, timeout time.Duration, opts ...registry.WatchOption) (registry.Watcher, error) {
	var wo registry.WatchOptions

	for _, o := range opts {
		o(&wo)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan bool, 1)

	go func() {
		<-stop
		cancel()
	}()

	watchPath := prefix
	if len(wo.Service) > 0 {
		watchPath = servicePath(wo.Service) + "/"
	}

	return &etcdWatcher{
		stop:    stop,
		w:       e.client.Watch(ctx, watchPath, clientv3.WithPrefix(), clientv3.WithPrevKV()),
		client:  e.client,
		timeout: timeout,
	}, nil
}

func (e *etcdWatcher) Next() (*registry.Result, error) {
	for wresp := range e.w {
		if wresp.Err() != nil {
			return nil, wresp.Err()
		}
		if wresp.Canceled {
			return nil, errors.New("cloud not get next")
		}

		for _, ev := range wresp.Events {
			service := decode(ev.Kv.Value)
			var action string

			switch ev.Type {
			case clientv3.EventTypePut:
				if ev.IsCreate() {
					action = "create"
				} else if ev.IsModify() {
					action = "update"
				}
			case clientv3.EventTypeDelete:
				action = "delete"
				service = decode(ev.PrevKv.Value)
			}
			if service == nil {
				continue
			}
			return &registry.Result{
				Action:  action,
				Service: service,
			}, nil
		}
	}
	return nil, errors.New("cloud not get next")
}

func (e *etcdWatcher) Stop() {
	select {
	case <-e.stop:
		return
	default:
		close(e.stop)
	}
}
