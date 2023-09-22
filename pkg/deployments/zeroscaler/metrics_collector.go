package zeroscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8s_types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	k8s "github.com/dailymotion-oss/osiris/pkg/kubernetes"
)

type metricsCollectorConfig struct {
	appKind                 string
	appName                 string
	appNamespace            string
	selector                labels.Selector
	metricsCheckInterval    time.Duration
	scraperConfig           metricsScraperConfig
	informerRefreshInterval time.Duration
}

type metricsCollector struct {
	config       metricsCollectorConfig
	scraper      metricsScraper
	kubeClient   kubernetes.Interface
	podsInformer cache.SharedIndexInformer
	appPods      map[string]*corev1.Pod
	appPodsLock  sync.Mutex
	cancelFunc   func()
}

func newMetricsCollector(
	kubeClient kubernetes.Interface,
	config metricsCollectorConfig,
) (*metricsCollector, error) {
	s, err := newMetricsScraper(config.scraperConfig)
	if err != nil {
		return nil, err
	}
	m := &metricsCollector{
		config:     config,
		scraper:    s,
		kubeClient: kubeClient,
		podsInformer: k8s.PodsIndexInformer(
			kubeClient,
			config.appNamespace,
			nil,
			config.selector,
			config.informerRefreshInterval,
		),
		appPods: map[string]*corev1.Pod{},
	}
	m.podsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: m.syncAppPod,
		UpdateFunc: func(_, newObj interface{}) {
			m.syncAppPod(newObj)
		},
		DeleteFunc: m.syncDeletedAppPod,
	})
	return m, nil
}

func (m *metricsCollector) run(ctx context.Context) {
	ctx, m.cancelFunc = context.WithCancel(ctx)
	defer m.cancelFunc()
	go func() {
		<-ctx.Done()
		glog.Infof(
			"Stopping metrics collection for %s %s in namespace %s",
			m.config.appKind,
			m.config.appName,
			m.config.appNamespace,
		)
	}()
	glog.Infof(
		"Starting metrics collection for %s %s in namespace %s",
		m.config.appKind,
		m.config.appName,
		m.config.appNamespace,
	)
	go m.podsInformer.Run(ctx.Done())
	// When this exits, the cancel func will stop the informer
	m.collectMetrics(ctx)
}

func (m *metricsCollector) stop() {
	m.cancelFunc()
}

func (m *metricsCollector) syncAppPod(obj interface{}) {
	m.appPodsLock.Lock()
	defer m.appPodsLock.Unlock()
	pod := obj.(*corev1.Pod)
	m.appPods[pod.Name] = pod
}

func (m *metricsCollector) syncDeletedAppPod(obj interface{}) {
	m.appPodsLock.Lock()
	defer m.appPodsLock.Unlock()
	pod := obj.(*corev1.Pod)
	delete(m.appPods, pod.Name)
}

func (m *metricsCollector) collectMetrics(ctx context.Context) {
	var (
		requestCountsByProxy     = map[string]uint64{}
		requestCountsByProxyLock sync.Mutex
		lastTotalRequestCount    uint64
		ticker                   = time.NewTicker(m.config.metricsCheckInterval)
	)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.appPodsLock.Lock()
			var (
				mustNotDecide bool
				scrapeWG      sync.WaitGroup
			)
			// An aggressively small timeout. We make the decision fast or not at
			// all.
			timer := time.NewTimer(3 * time.Second)
			for _, pod := range m.appPods {
				scrapeWG.Add(1)
				go func(pod *corev1.Pod) {
					defer scrapeWG.Done()
					// Get the results
					prc := m.scraper.Scrape(pod)
					if prc == nil {
						mustNotDecide = true
					} else {
						requestCountsByProxyLock.Lock()
						requestCountsByProxy[prc.ProxyID] = prc.RequestCount
						requestCountsByProxyLock.Unlock()
					}
				}(pod)
			}
			m.appPodsLock.Unlock()
			scrapeWG.Wait()
			var totalRequestCount uint64
			for _, requestCount := range requestCountsByProxy {
				totalRequestCount += requestCount
			}
			select {
			case <-timer.C:
				mustNotDecide = true
			case <-ctx.Done():
				return
			default:
			}
			timer.Stop()
			if !mustNotDecide && totalRequestCount == lastTotalRequestCount {
				m.scaleToZero(context.TODO())
			}
			lastTotalRequestCount = totalRequestCount
		case <-ctx.Done():
			return
		}
	}
}

func (m *metricsCollector) scaleToZero(ctx context.Context) {
	// scale the main app to zero first
	scaleToZero(ctx, m.kubeClient, m.config.appKind, m.config.appNamespace, m.config.appName)

	// and then the dependencies - if any
	var dependenciesAnnotationValue string
	switch strings.ToLower(m.config.appKind) {
	case "deployment":
		deployment, err := m.kubeClient.AppsV1().Deployments(m.config.appNamespace).Get(ctx, m.config.appName, metav1.GetOptions{})
		if err != nil {
			glog.Errorf("Error retrieving deployment %s in namespace %s: %s", m.config.appName, m.config.appNamespace, err)
			return
		}
		if deployment.Annotations != nil {
			dependenciesAnnotationValue = cleanAnnotationValue(deployment.Annotations["osiris.dm.gg/dependencies"])
		}
	case "statefulset":
		statefulset, err := m.kubeClient.AppsV1().StatefulSets(m.config.appNamespace).Get(ctx, m.config.appName, metav1.GetOptions{})
		if err != nil {
			glog.Errorf("Error retrieving statefulset %s in namespace %s: %s", m.config.appName, m.config.appNamespace, err)
			return
		}
		if statefulset.Annotations != nil {
			dependenciesAnnotationValue = cleanAnnotationValue(statefulset.Annotations["osiris.dm.gg/dependencies"])
		}
	}

	for _, dependency := range strings.Split(dependenciesAnnotationValue, ",") {
		if len(dependency) == 0 {
			continue
		}
		elems := strings.SplitN(dependency, ":", 2)
		depKind := elems[0]
		elems = strings.SplitN(elems[1], "/", 2)
		depNamespace := elems[0]
		depName := elems[1]
		scaleToZero(ctx, m.kubeClient, depKind, depNamespace, depName)
	}
}

func scaleToZero(ctx context.Context, kubeClient kubernetes.Interface, kind, namespace, name string) {
	glog.Infof("Scale to zero starting for %s %s in namespace %s", kind, name, namespace)

	patches := []k8s.PatchOperation{{
		Op:    "replace",
		Path:  "/spec/replicas",
		Value: 0,
	}}
	patchesBytes, _ := json.Marshal(patches)
	var err error
	switch strings.ToLower(kind) {
	case "deployment":
		_, err = kubeClient.AppsV1().Deployments(namespace).Patch(
			ctx,
			name,
			k8s_types.JSONPatchType,
			patchesBytes,
			metav1.PatchOptions{},
		)
	case "statefulset":
		_, err = kubeClient.AppsV1().StatefulSets(namespace).Patch(
			ctx,
			name,
			k8s_types.JSONPatchType,
			patchesBytes,
			metav1.PatchOptions{},
		)
	default:
		err = fmt.Errorf("unknown kind '%s'", kind)
	}
	if err != nil {
		glog.Errorf("Error scaling %s %s in namespace %s to zero: %s", kind, name, namespace, err)
		return
	}

	glog.Infof("Scaled %s %s in namespace %s to zero", kind, name, namespace)
}

func cleanAnnotationValue(rawValue string) string {
	value := strings.TrimSpace(rawValue)
	value = strings.TrimLeft(value, "'")
	value = strings.TrimRight(value, "'")
	return value
}
