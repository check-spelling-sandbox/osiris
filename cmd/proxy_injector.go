package cmd

import (
	"context"

	"github.com/golang/glog"

	proxy "github.com/dailymotion-oss/osiris/pkg/metrics/proxy/injector"
	"github.com/dailymotion-oss/osiris/pkg/version"
)

func RunProxyInjector(ctx context.Context) {
	glog.Infof(
		"Starting Osiris Proxy Injector -- version %s -- commit %s",
		version.Version(),
		version.Commit(),
	)

	cfg, err := proxy.GetConfigFromEnvironment()
	if err != nil {
		glog.Fatalf(
			"Error retrieving proxy injector configuration: %s",
			err,
		)
	}

	// Run the proxy injector
	proxy.NewInjector(cfg).Run(ctx)
}
