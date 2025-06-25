package mongoproxy

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"
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
func resolveTarget(targetConnString *connstring.ConnString) (string, error) {
	if strings.EqualFold(targetConnString.Scheme, "mongodb+srv") {
		panic("no support for mongodb+srv yet")
	}

	primaryAddr, err := findPrimary(targetConnString.Original, targetConnString.Hosts)
	if err != nil {
		return "", fmt.Errorf("failed to find primary: %w", err)
	}

	return primaryAddr, nil
}

// findPrimary takes the original URI (so we can preserver options) and list of
// host:port addresses to check; it returns the one where a directConnection
// hello reports isWritablePrimary=true.
func findPrimary(baseURI string, hosts []string) (string, error) {
	for _, h := range hosts {
		u, err := url.Parse(baseURI)
		if err != nil {
			continue
		}

		u.Host = h
		//u.Path = "/"
		//q := u.Query()
		//u.RawQuery = q.Encode()

		client, err := mongo.Connect(options.Client().ApplyURI(u.String()))
		if err != nil {
			continue
		}

		var res struct {
			Primary           string `bson:"primary"`
			IsWritablePrimary bool   `bson:"isWritablePrimary"`
		}

		cmd := bson.D{{Key: "hello", Value: 1}}

		if err := client.Database("admin").RunCommand(context.Background(), cmd).Decode(&res); err != nil {
			client.Disconnect(context.Background())
			continue
		}

		client.Disconnect(context.Background())
		if res.IsWritablePrimary {
			return h, nil
		}

		if res.Primary != "" {
			return res.Primary, nil
		}
	}

	return "", fmt.Errorf("no primary found in %v", hosts)
}
