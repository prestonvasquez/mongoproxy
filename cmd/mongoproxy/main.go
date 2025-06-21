package main

import (
	"flag"

	"github.com/prestonvasquez/mongoproxy"
)

func main() {
	// Optional flags. Leave them blank/zero to keep library defaults.
	listen := flag.String("listen", "", "proxy listen address, e.g. :27018 (default: library default)")
	target := flag.String("target", "", "upstream MongoDB address, e.g. localhost:27017 (default: library default)")
	targetURI := flag.String("target-uri", "", "upstream MongoDB URI, e.g. mongodb://localhost:27017 (default: library default)")

	flag.Parse()

	// Build functional options only for flags the user actually set.
	var opts []mongoproxy.Option
	if *listen != "" {
		opts = append(opts, mongoproxy.WithListenAddr(*listen))
	}
	if *target != "" {
		opts = append(opts, mongoproxy.WithTargetAddr(*target))
	}
	if *targetURI != "" {
		opts = append(opts, mongoproxy.WithTargetURI(*targetURI))
	}

	// Start the proxy.
	mongoproxy.ListenAndServe(opts...)
}
