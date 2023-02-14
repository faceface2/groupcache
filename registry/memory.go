package registry

import (
	"github.com/google/uuid"
	"sync"
	"time"
)

var (
	sendEventTime = 10 * time.Millisecond
	ttlPruneTime  = time.Second
)

type node struct {
	*Node
	TTL      time.Duration
	LastSeen time.Time
}

type record struct {
	Name      string
	Version   string
	Metadata  map[string]string
	Nodes     map[string]*node
	Endpoints []*Endpoint
}

type memRegistry struct {
	options *Options

	sync.RWMutex
	records  map[string]map[string]*record
	watchers map[string]*memWatcher
}

func (m *memRegistry) Init(opts ...Option) error {
	for _, o := range opts {
		o(m.options)
	}
	m.Lock()
	defer m.Unlock()

	records := getServiceRecords(m.options.Context)
	for name, record := range records {
		if _, ok := m.records[name]; !ok {
			m.records[name] = record
			continue
		}

		for version, r := range record {
			if _, ok := m.records[name][version]; !ok {
				m.records[name][version] = r
				continue
			}
		}
	}
	return nil
}

func (m *memRegistry) Options() Options {
	return *m.options
}

func (m *memRegistry) Register(service *Service, opts ...RegisterOption) error {
	m.Lock()
	defer m.Unlock()

	logger := m.options.Logger
	var options RegisterOptions

	for _, o := range opts {
		o(&options)
	}

	r := serviceToRecord(service, options.TTL)
	if _, ok := m.records[service.Name]; !ok {
		m.records[service.Name] = make(map[string]*record)
	}

	if _, ok := m.records[service.Name][service.Version]; !ok {
		m.records[service.Name][service.Version] = r
		logger.Printf("Registry added new service: %s, version: %s", service.Name, service.Version)
		go m.sendEvent(&Result{Action: "update", Service: service})
		return nil
	}

	addedNodes := false

	for _, n := range service.Nodes {
		if _, ok := m.records[service.Name][service.Version].Nodes[n.ID]; !ok {
			addedNodes = true
			metadata := make(map[string]string)
			for k, v := range n.Metadata {
				metadata[k] = v
			}
			m.records[service.Name][service.Version].Nodes[n.ID] = &node{
				Node: &Node{
					ID:       n.ID,
					Address:  n.Address,
					Metadata: n.Metadata,
				},
				TTL:      options.TTL,
				LastSeen: time.Now(),
			}
		}
	}
	if addedNodes {
		logger.Printf("Registry added new node to service: %s, version: %s", service.Name, service.Version)
		go m.sendEvent(&Result{Action: "update", Service: service})
		return nil
	}

	for _, n := range service.Nodes {
		logger.Printf("Updated registration for service: %s, version: %s", service.Name, service.Version)
		m.records[service.Name][service.Version].Nodes[n.ID].TTL = options.TTL
		m.records[service.Name][service.Version].Nodes[n.ID].LastSeen = time.Now()
	}
	return nil
}

func (m *memRegistry) Deregister(service *Service, option ...DeregisterOption) error {
	m.Lock()
	defer m.Unlock()
	logger := m.options.Logger
	if _, ok := m.records[service.Name]; ok {
		if _, ok := m.records[service.Name][service.Version]; ok {
			for _, n := range service.Nodes {
				if _, ok := m.records[service.Name][service.Version].Nodes[n.ID]; ok {
					logger.Printf("Registry removed node from service: %s, version: %s", service.Name, service.Version)
					delete(m.records[service.Name][service.Version].Nodes, n.ID)
				}
			}
			if len(m.records[service.Name][service.Version].Nodes) == 0 {
				delete(m.records[service.Name], service.Version)
				logger.Printf("Registry removed service: %s, version: %s", service.Name, service.Version)
			}
		}
		if len(m.records[service.Name]) == 0 {
			delete(m.records, service.Name)
			logger.Printf("Registry removed service: %s", service.Name)
		}
		go m.sendEvent(&Result{Action: "delete", Service: service})
	}
	return nil
}

func (m *memRegistry) GetService(name string, opts ...GetOption) ([]*Service, error) {
	m.RLock()
	defer m.RUnlock()
	records, ok := m.records[name]
	if !ok {
		return nil, ErrNotFound
	}
	services := make([]*Service, len(m.records[name]))
	i := 0
	for _, record := range records {
		services[i] = recordToService(record)
		i++
	}
	return services, nil
}

func (m *memRegistry) ListServices(option ...ListOption) ([]*Service, error) {
	m.RLock()
	defer m.RUnlock()
	var services []*Service
	for _, records := range m.records {
		for _, record := range records {
			services = append(services, recordToService(record))
		}
	}
	return services, nil
}

func (m *memRegistry) Watch(option ...WatchOption) (Watcher, error) {
	var wo WatchOptions
	for _, o := range option {
		o(&wo)
	}

	w := &memWatcher{
		exit: make(chan bool),
		res:  make(chan *Result),
		id:   uuid.New().String(),
		wo:   wo,
	}

	m.Lock()
	m.watchers[w.id] = w
	m.Unlock()
	return w, nil
}

func (m *memRegistry) String() string {
	return "memory"
}

func NewMemoryRegistry(opts ...Option) Registry {
	options := NewOptions(opts...)

	records := getServiceRecords(options.Context)
	if records == nil {
		records = make(map[string]map[string]*record)
	}
	reg := &memRegistry{
		options:  options,
		records:  records,
		watchers: make(map[string]*memWatcher),
	}
	go reg.ttlPrune()
	return reg
}

func (m *memRegistry) ttlPrune() {
	logger := m.options.Logger
	prune := time.NewTicker(ttlPruneTime)
	defer prune.Stop()

	for true {
		select {
		case <-prune.C:
			m.Lock()
			for name, records := range m.records {
				for version, record := range records {
					for id, n := range record.Nodes {
						if n.TTL != 0 && time.Since(n.LastSeen) > n.TTL {
							logger.Printf("Registry TTL expired for node %s of service %s", n.ID, name)
							delete(m.records[name][version].Nodes, id)
						}
					}
				}
			}
			m.Unlock()
		}
	}
}

func (m *memRegistry) sendEvent(r *Result) {
	m.RLock()
	watchers := make([]*memWatcher, 0, len(m.watchers))
	for _, w := range m.watchers {
		watchers = append(watchers, w)
	}
	m.RUnlock()

	for _, w := range watchers {
		select {
		case <-w.exit:
			m.Lock()
			delete(m.watchers, w.id)
			m.Unlock()
		default:
			select {
			case w.res <- r:
			case <-time.After(sendEventTime):
			}
		}
	}

}

func recordToService(r *record) *Service {
	endpoints := make([]*Endpoint, len(r.Endpoints))
	for i, e := range r.Endpoints {
		request := new(Value)
		if e.Request != nil {
			*request = *e.Request
		}
		response := new(Value)
		if e.Request != nil {
			*response = *e.Response
		}
		metadata := make(map[string]string, len(e.Metadata))
		for k, v := range e.Metadata {
			metadata[k] = v
		}
		endpoints[i] = &Endpoint{
			Name:     e.Name,
			Request:  request,
			Response: response,
			Metadata: metadata,
		}
	}
	metadata := make(map[string]string, len(r.Metadata))
	for k, v := range r.Metadata {
		metadata[k] = v
	}

	nodes := make([]*Node, len(r.Nodes))
	i := 0
	for _, n := range r.Nodes {
		metadata := make(map[string]string, len(n.Metadata))
		for k, v := range n.Metadata {
			metadata[k] = v
		}
		nodes[i] = &Node{
			ID:       n.ID,
			Address:  n.Address,
			Metadata: metadata,
		}
		i++
	}
	return &Service{
		Name:      r.Name,
		Version:   r.Version,
		Nodes:     nodes,
		Metadata:  metadata,
		Endpoints: endpoints,
	}
}

func serviceToRecord(s *Service, ttl time.Duration) *record {
	metadata := make(map[string]string, len(s.Metadata))

	for k, v := range s.Metadata {
		metadata[k] = v
	}

	nodes := make(map[string]*node, len(s.Nodes))
	for _, n := range s.Nodes {
		nodes[n.ID] = &node{
			Node:     n,
			TTL:      ttl,
			LastSeen: time.Now(),
		}
	}

	endpoints := make([]*Endpoint, len(s.Endpoints))
	for i, e := range s.Endpoints {
		endpoints[i] = e
	}

	return &record{
		Name:      s.Name,
		Version:   s.Version,
		Metadata:  metadata,
		Nodes:     nodes,
		Endpoints: endpoints,
	}
}
