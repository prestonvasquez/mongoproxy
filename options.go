package mongoproxy

type Config struct {
	ListenAddr string // Address to listen for incoming connections
	TargetAddr string // Address of the target MongoDB server
}

// Option defines a function type that modifies the Config.
type Option func(*Config)

// WithListenAddr sets the address to listen for incoming connections.
func WithListenAddr(addr string) Option {
	return func(cfg *Config) {
		cfg.ListenAddr = addr
	}
}

// WithTargetAddr sets the address of the target MongoDB server.
func WithTargetAddr(addr string) Option {
	return func(cfg *Config) {
		cfg.TargetAddr = addr
	}
}
