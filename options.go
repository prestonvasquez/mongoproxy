package mongoproxy

import (
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/dns"
)

type Config struct {
	ListenAddr string // Address to listen for incoming connections
	TargetAddr string // Address of the target MongoDB server
	TargetURI  string // URI of the target MongoDB server
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

// WithTargetURI sets the URI of the target MongoDB server.
func WithTargetURI(uri string) Option {
	return func(cfg *Config) {
		cfg.TargetURI = uri
	}
}

// resolveTarget chooses between plain host:port or parses a Mongo URI.
//
// TODO: Likely for the SRV solution to work we will need to perform hello
// TODO: commands against each node to determine which one is the primary.
func resolveTarget(cfg Config) (string, bool, error) {
	if cfg.TargetURI != "" {
		cs, err := connstring.Parse(cfg.TargetURI)
		if err != nil {
			return "", false, err
		}

		if strings.EqualFold(cs.Scheme, "mongodb+srv") {
			host := cs.RawHosts[0]
			addrList, err := dns.DefaultResolver.ParseHosts(host, cs.SRVServiceName, true)
			if err != nil {
				return "", false, fmt.Errorf("failed to resolve SRV record for %s: %w", host, err)
			}

			return addrList[0], true, nil
		}

		return cs.Hosts[0], false, nil
	}

	return cfg.TargetAddr, false, nil
}
