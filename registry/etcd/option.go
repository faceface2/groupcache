package etcd

import (
	"context"
	"go.uber.org/zap"
	"groupcache/registry"
)

type authKey struct{}

type authCreds struct {
	Username string
	Password string
}
type logConfigKey struct{}

func Auth(username, password string) registry.Option {
	return func(o *registry.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, authKey{}, &authCreds{Username: username, Password: password})
	}
}

// LogConfig allows you to set etcd log config.
func LogConfig(config *zap.Config) registry.Option {
	return func(o *registry.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, logConfigKey{}, config)
	}
}
