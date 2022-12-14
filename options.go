package firestore

import "go.uber.org/zap"

type Option func(*Config) error

func WithLogger(logger *zap.Logger) Option {
	return func(conf *Config) error {
		conf.Logger = logger
		return nil
	}
}

func WithCollection(key string) Option {
	return func(conf *Config) error {
		conf.Collection = key
		return nil
	}
}
