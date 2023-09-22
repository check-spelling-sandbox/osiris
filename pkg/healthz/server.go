package healthz

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"net/http/pprof"

	"github.com/golang/glog"
)

func RunServer(ctx context.Context, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", HandleHealthCheckRequest)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	doneCh := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done(): // Context was canceled or expired
			glog.Info("Healthz server is shutting down")
			// Allow up to five seconds for requests in progress to be completed
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*5,
			)
			defer cancel()
			srv.Shutdown(shutdownCtx) // nolint: errcheck
		case <-doneCh: // The server shut down on its own, perhaps due to an error
		}
	}()

	glog.Infof("Healthz server is listening on %s", srv.Addr)
	err := srv.ListenAndServe()
	if err != http.ErrServerClosed {
		glog.Errorf("Healthz server error: %s", err)
		err = nil
	}
	close(doneCh)
}
