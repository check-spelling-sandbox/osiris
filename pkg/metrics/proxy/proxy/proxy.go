package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	uuid "github.com/satori/go.uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"

	"github.com/dailymotion-oss/osiris/pkg/healthz"
	"github.com/dailymotion-oss/osiris/pkg/metrics"
	"github.com/dailymotion-oss/osiris/pkg/version"
)

type Proxy interface {
	Run(ctx context.Context)
}

type proxy struct {
	proxyID               string
	requestCount          *uint64
	singlePortProxies     []*singlePortProxy
	healthzAndMetricsSvr  *http.Server
	ignoredPaths          map[string]struct{}
	openTelemetryExporter *otlp.Exporter
}

func NewProxy(cfg Config) (Proxy, error) {
	var requestCount uint64
	healthzAndMetricsMux := http.NewServeMux()
	p := &proxy{
		proxyID:           uuid.NewV4().String(),
		requestCount:      &requestCount,
		singlePortProxies: []*singlePortProxy{},
		healthzAndMetricsSvr: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.MetricsAndHealthPort),
			Handler: healthzAndMetricsMux,
		},
		ignoredPaths: cfg.IgnoredPaths,
	}
	p.initOpenTelemetry(cfg.OpenTelemetryEndpoint)
	for proxyPort, appPort := range cfg.PortMappings {
		singlePortProxy, err :=
			newSinglePortProxy(proxyPort, appPort, p.requestCount, p.ignoredPaths)
		if err != nil {
			return nil, err
		}
		p.singlePortProxies = append(
			p.singlePortProxies,
			singlePortProxy,
		)
	}
	healthzAndMetricsMux.HandleFunc("/metrics", p.handleMetricsRequest)
	healthzAndMetricsMux.HandleFunc("/healthz", healthz.HandleHealthCheckRequest)
	return p, nil
}

func (p *proxy) Run(ctx context.Context) {
	if p.openTelemetryExporter != nil {
		defer func() {
			err := p.openTelemetryExporter.Shutdown(ctx)
			if err != nil {
				glog.Errorf("failed to stop open telemetry exporter: %s", err)
			}
		}()
	}

	ctx, cancel := context.WithCancel(ctx)

	// Start proxies for each port
	for _, spp := range p.singlePortProxies {
		go func(spp *singlePortProxy) {
			spp.run(ctx)
			cancel()
		}(spp)
	}

	doneCh := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done(): // Context was canceled or expired
			glog.Info("Healthz and metrics server is shutting down")
			// Allow up to five seconds for requests in progress to be completed
			shutdownCtx, shutdownCancel := context.WithTimeout(
				context.Background(),
				time.Second*5,
			)
			defer shutdownCancel()
			p.healthzAndMetricsSvr.Shutdown(shutdownCtx) // nolint: errcheck
		case <-doneCh: // The server shut down on its own, perhaps due to an error
		}
		cancel()
	}()

	glog.Infof(
		"Healthz and metrics server is listening on %s",
		p.healthzAndMetricsSvr.Addr,
	)
	err := p.healthzAndMetricsSvr.ListenAndServe()
	if err != http.ErrServerClosed {
		glog.Errorf("Error from healthz and metrics server: %s", err)
	}
	close(doneCh)
}

func (p *proxy) handleMetricsRequest(w http.ResponseWriter, _ *http.Request) {
	prc := metrics.ProxyRequestCount{
		ProxyID:      p.proxyID,
		RequestCount: atomic.LoadUint64(p.requestCount),
	}
	prcBytes, err := json.Marshal(prc)
	if err != nil {
		glog.Errorf("Error marshaling metrics request response: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(prcBytes); err != nil {
		glog.Errorf("Error writing metrics request response body: %s", err)
	}
}

func (p *proxy) initOpenTelemetry(openTelemetryEndpoint string) {
	if len(openTelemetryEndpoint) == 0 {
		return
	}

	ctx := context.Background()
	exporter, err := otlp.NewExporter(ctx, otlpgrpc.NewDriver(
		otlpgrpc.WithEndpoint(openTelemetryEndpoint),
		otlpgrpc.WithInsecure(),
	))
	if err != nil {
		glog.Warningf("failed to create an opentelemetry exporter for GRPC insecure endpoint %s: %s", openTelemetryEndpoint, err)
		return
	}

	p.openTelemetryExporter = exporter

	resource, err := sdkresource.Detect(ctx, &gcp.GKE{})
	if err != nil {
		glog.Warningf("failed to detect telemetry resource: %s", err)
	}
	resource = sdkresource.Merge(resource, sdkresource.NewWithAttributes(
		semconv.ServiceNameKey.String("osiris-proxy"),
		semconv.ServiceVersionKey.String(version.Version()),
		label.Key("pod").String(os.Getenv("HOSTNAME")),
		label.Key("container").String("osiris-proxy"),
	))
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
		sdktrace.WithResource(resource),
	))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.Baggage{},
		propagation.TraceContext{},
	))
}
