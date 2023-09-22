package activator

import (
	"context"
	"net/http"
	"time"

	"github.com/golang/glog"
)

func (a *activator) runServer(ctx context.Context, srv *http.Server) error {
	doneCh := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done(): // Context was canceled or expired
			glog.Info("http server is shutting down")
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

	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		err = nil
	}
	close(doneCh)
	return err
}
