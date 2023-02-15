package registry

import (
	"context"
	"crypto/tls"
	"go.uber.org/zap"
	"log"
	"time"
)

type Options struct {
	Addrs     []string
	Timeout   time.Duration
	Secure    bool
	TLSConfig *tls.Config
	Logger    *zap.Logger
	Context   context.Context
}

type RegisterOptions struct {
	TTL     time.Duration
	Context context.Context
}

type WatchOptions struct {
	Service string

	Context context.Context
}

type DeregisterOptions struct {
	Context context.Context
}

type GetOptions struct {
	Context context.Context
}

type ListOptions struct {
	Context context.Context
}

func NewOptions(opts ...Option) *Options {
	options := Options{
		Context: context.Background(),
		Logger:  log.Default(),
	}

	for _, o := range opts {
		o(&options)
	}
	return &options
}

func Addrs(addrs ...string) Option {
	return func(options *Options) {
		options.Addrs = addrs
	}
}

func Timeout(t time.Duration) Option {
	return func(options *Options) {
		options.Timeout = t
	}
}

func Secure(b bool) Option {
	return func(options *Options) {
		options.Secure = b
	}
}

func TLSConfig(t *tls.Config) Option {
	return func(options *Options) {
		options.TLSConfig = t
	}
}

func RegisterTTL(t time.Duration) RegisterOption {
	return func(options *RegisterOptions) {
		options.TTL = t
	}
}

func RegisterContext(context context.Context) RegisterOption {
	return func(options *RegisterOptions) {
		options.Context = context
	}
}

func WatchService(name string) WatchOption {
	return func(options *WatchOptions) {
		options.Service = name
	}
}

func WatchContext(context context.Context) WatchOption {
	return func(options *WatchOptions) {
		options.Context = context
	}
}

func DeregisterContext(ctx context.Context) DeregisterOption {
	return func(options *DeregisterOptions) {
		options.Context = ctx
	}
}

func GetContext(ctx context.Context) GetOption {
	return func(options *GetOptions) {
		options.Context = ctx
	}
}

func ListContext(ctx context.Context) ListOption {
	return func(options *ListOptions) {
		options.Context = ctx
	}
}

type servicesKey struct{}

func getServiceRecords(ctx context.Context) map[string]map[string]*record {
	memServices, ok := ctx.Value(servicesKey{}).(map[string][]*Service)
	if !ok {
		return nil
	}
	services := make(map[string]map[string]*record)
	for name, svc := range memServices {
		if _, ok := services[name]; !ok {
			services[name] = make(map[string]*record)
		}
		for _, s := range svc {
			services[s.Name][s.Version] = serviceToRecord(s, 0)
		}
	}
	return services
}

func Services(s map[string][]*Service) Option {
	return func(options *Options) {
		if options.Context == nil {
			options.Context = context.Background()
		}
		options.Context = context.WithValue(options.Context, servicesKey{}, s)
	}
}

func Logger(l *log.Logger) Option {
	return func(options *Options) {
		options.Logger = l
	}
}
