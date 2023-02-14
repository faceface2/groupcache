package registry

import (
	"errors"
	"time"
)

type Watcher interface {
	Next() (*Result, error)
	Stop()
}

type Result struct {
	Action  string
	Service *Service
}

type EventType int

const (
	Create EventType = iota
	Delete
	Update
)

func (t EventType) String() string {
	switch t {
	case Create:
		return "create"
	case Delete:
		return "delete"
	case Update:
		return "update"
	default:
		return "unknown"
	}
}

type Event struct {
	Id   string
	Type EventType

	Timestamp time.Time

	Service *Service
}

type memWatcher struct {
	id   string
	wo   WatchOptions
	res  chan *Result
	exit chan bool
}

func (m *memWatcher) Next() (*Result, error) {
	for {
		select {
		case r := <-m.res:
			if len(m.wo.Service) > 0 && m.wo.Service != r.Service.Name {
				continue
			}
			return r, nil
		case <-m.exit:
			return nil, errors.New("watcher stopped")
		}
	}
}

func (m *memWatcher) Stop() {
	select {
	case <-m.exit:
		return
	default:
		close(m.exit)
	}
}
