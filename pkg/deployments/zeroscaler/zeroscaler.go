package zeroscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/dailymotion-oss/osiris/pkg/healthz"
	k8s "github.com/dailymotion-oss/osiris/pkg/kubernetes"
)

type Zeroscaler interface {
	Run(ctx context.Context)
}

type zeroscaler struct {
	cfg                  Config
	kubeClient           kubernetes.Interface
	deploymentsInformer  cache.SharedInformer
	statefulSetsInformer cache.SharedInformer
	collectors           map[string]*metricsCollector
	collectorsLock       sync.Mutex
	ctx                  context.Context
}

func NewZeroscaler(cfg Config, kubeClient kubernetes.Interface) Zeroscaler {
	z := &zeroscaler{
		cfg:        cfg,
		kubeClient: kubeClient,
		deploymentsInformer: k8s.DeploymentsIndexInformer(
			kubeClient,
			metav1.NamespaceAll,
			nil,
			nil,
			cfg.ResyncInterval,
		),
		statefulSetsInformer: k8s.StatefulSetsIndexInformer(
			kubeClient,
			metav1.NamespaceAll,
			nil,
			nil,
			cfg.ResyncInterval,
		),
		collectors: map[string]*metricsCollector{},
	}
	z.deploymentsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: z.syncDeployment,
		UpdateFunc: func(_, newObj interface{}) {
			z.syncDeployment(newObj)
		},
		DeleteFunc: z.syncDeletedDeployment,
	})
	z.statefulSetsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: z.syncStatefulSet,
		UpdateFunc: func(_, newObj interface{}) {
			z.syncStatefulSet(newObj)
		},
		DeleteFunc: z.syncDeletedStatefulSet,
	})
	return z
}

// Run causes the controller to collect metrics for Osiris-enabled deployments and statefulsets.
func (z *zeroscaler) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	z.ctx = ctx
	go func() {
		<-ctx.Done()
		glog.Infof("Zeroscaler is shutting down")
	}()
	glog.Infof("Zeroscaler is started")
	go func() {
		z.deploymentsInformer.Run(ctx.Done())
		cancel()
	}()
	go func() {
		z.statefulSetsInformer.Run(ctx.Done())
		cancel()
	}()
	healthz.RunServer(ctx, 5000)
	cancel()
}

func (z *zeroscaler) syncDeployment(obj interface{}) {
	deployment := obj.(*appsv1.Deployment)
	if k8s.WorkloadIsEligibleForAutoScaling(deployment.Annotations) {
		glog.Infof(
			"Notified about new or updated Osiris-enabled deployment %s in "+
				"namespace %s",
			deployment.Name,
			deployment.Namespace,
		)
		minReplicas := k8s.GetMinReplicas(deployment.Annotations, 1)
		if *deployment.Spec.Replicas > 0 &&
			deployment.Status.AvailableReplicas <= minReplicas {
			glog.Infof(
				"Osiris-enabled deployment %s in namespace %s is running the minimum "+
					"number of replicas or fewer; ensuring metrics collection",
				deployment.Name,
				deployment.Namespace,
			)
			z.ensureMetricsCollection(
				"Deployment",
				deployment.Namespace,
				deployment.Name,
				deployment.Annotations,
				deployment.Spec.Selector,
			)
		} else {
			glog.Infof(
				"Osiris-enabled deployment %s in namespace %s is running zero "+
					"replicas OR more than the minimum number of replicas; ensuring "+
					"NO metrics collection",
				deployment.Name,
				deployment.Namespace,
			)
			z.ensureNoMetricsCollection(
				"Deployment",
				deployment.Namespace,
				deployment.Name,
			)
		}
	} else {
		glog.Infof(
			"Notified about new or updated non-Osiris-enabled deployment %s in "+
				"namespace %s; ensuring NO metrics collection",
			deployment.Name,
			deployment.Namespace,
		)
		z.ensureNoMetricsCollection(
			"Deployment",
			deployment.Namespace,
			deployment.Name,
		)
	}
}

func (z *zeroscaler) syncStatefulSet(obj interface{}) {
	statefulSet := obj.(*appsv1.StatefulSet)
	if k8s.WorkloadIsEligibleForAutoScaling(statefulSet.Annotations) {
		glog.Infof(
			"Notified about new or updated Osiris-enabled statefulSet %s in "+
				"namespace %s",
			statefulSet.Name,
			statefulSet.Namespace,
		)
		minReplicas := k8s.GetMinReplicas(statefulSet.Annotations, 1)
		if *statefulSet.Spec.Replicas > 0 &&
			statefulSet.Status.ReadyReplicas <= minReplicas {
			glog.Infof(
				"Osiris-enabled statefulSet %s in namespace %s is running the minimum "+
					"number of replicas or fewer; ensuring metrics collection",
				statefulSet.Name,
				statefulSet.Namespace,
			)
			z.ensureMetricsCollection(
				"StatefulSet",
				statefulSet.Namespace,
				statefulSet.Name,
				statefulSet.Annotations,
				statefulSet.Spec.Selector,
			)
		} else {
			glog.Infof(
				"Osiris-enabled statefulSet %s in namespace %s is running zero "+
					"replicas OR more than the minimum number of replicas; ensuring "+
					"NO metrics collection",
				statefulSet.Name,
				statefulSet.Namespace,
			)
			z.ensureNoMetricsCollection(
				"StatefulSet",
				statefulSet.Namespace,
				statefulSet.Name,
			)
		}
	} else {
		glog.Infof(
			"Notified about new or updated non-Osiris-enabled statefulSet %s in "+
				"namespace %s; ensuring NO metrics collection",
			statefulSet.Name,
			statefulSet.Namespace,
		)
		z.ensureNoMetricsCollection(
			"StatefulSet",
			statefulSet.Namespace,
			statefulSet.Name,
		)
	}
}

func (z *zeroscaler) syncDeletedDeployment(obj interface{}) {
	deployment := obj.(*appsv1.Deployment)
	glog.Infof(
		"Notified about deleted deployment %s in namespace %s; ensuring NO "+
			"metrics collection",
		deployment.Name,
		deployment.Namespace,
	)
	z.ensureNoMetricsCollection(
		"Deployment",
		deployment.Namespace,
		deployment.Name,
	)
}

func (z *zeroscaler) syncDeletedStatefulSet(obj interface{}) {
	statefulSet := obj.(*appsv1.StatefulSet)
	glog.Infof(
		"Notified about deleted statefulSet %s in namespace %s; ensuring NO "+
			"metrics collection",
		statefulSet.Name,
		statefulSet.Namespace,
	)
	z.ensureNoMetricsCollection(
		"StatefulSet",
		statefulSet.Namespace,
		statefulSet.Name,
	)
}

func (z *zeroscaler) ensureMetricsCollection(kind, namespace, name string,
	annotations map[string]string, labelSelector *metav1.LabelSelector) {
	z.collectorsLock.Lock()
	defer z.collectorsLock.Unlock()
	key := getKey(kind, namespace, name)
	config := metricsCollectorConfig{
		appKind:                 kind,
		appName:                 name,
		appNamespace:            namespace,
		selector:                labels.SelectorFromSet(labelSelector.MatchLabels),
		scraperConfig:           getMetricsScraperConfig(kind, name, annotations),
		metricsCheckInterval:    z.getMetricsCheckInterval(kind, name, annotations),
		informerRefreshInterval: z.cfg.ResyncInterval,
	}
	if collector, ok := z.collectors[key]; !ok ||
		!reflect.DeepEqual(config, collector.config) {
		if ok {
			collector.stop()
		}
		glog.Infof(
			"Using new metrics collector for %s %s in namespace %s "+
				"with metrics check interval of %s",
			kind,
			name,
			namespace,
			config.metricsCheckInterval.String(),
		)
		collector, err := newMetricsCollector(z.kubeClient, config)
		if err != nil {
			glog.Errorf(
				"Metrics collector for %s %s in namespace %s can't run; "+
					"error: %s",
				kind,
				name,
				namespace,
				err,
			)
			return
		}
		go func() {
			collector.run(z.ctx)
			// Once the collector has run to completion (scaled to zero) remove it
			// from the map
			z.collectorsLock.Lock()
			defer z.collectorsLock.Unlock()
			delete(z.collectors, key)
		}()
		z.collectors[key] = collector
		return
	}
	glog.Infof(
		"Using existing metrics collector for %s %s in namespace %s",
		kind,
		name,
		namespace,
	)
}

func (z *zeroscaler) ensureNoMetricsCollection(kind, namespace, name string) {
	z.collectorsLock.Lock()
	defer z.collectorsLock.Unlock()
	key := getKey(kind, namespace, name)
	if collector, ok := z.collectors[key]; ok {
		collector.stop()
		delete(z.collectors, key)
	}
}

func getMetricsScraperConfig(
	kind string,
	name string,
	annotations map[string]string,
) metricsScraperConfig {
	rawConfig, found := annotations[k8s.MetricsCollectorAnnotationName]
	if !found {
		return metricsScraperConfig{ScraperName: osirisScraperName}
	}
	var config metricsScraperConfig
	if err := json.Unmarshal([]byte(rawConfig), &config); err != nil {
		fmt.Printf(
			"There was an error parsing metrics collector configuration "+
				"from %s %s, falling back to the default config; "+
				"error: %s",
			kind,
			name,
			err,
		)
		return metricsScraperConfig{ScraperName: osirisScraperName}
	}
	return config
}

func (z *zeroscaler) getMetricsCheckInterval(
	kind string,
	name string,
	annotations map[string]string,
) time.Duration {
	var (
		metricsCheckInterval int
		err                  error
	)
	if rawMetricsCheckInterval, ok :=
		annotations[k8s.MetricsCheckIntervalAnnotationName]; ok {
		metricsCheckInterval, err = strconv.Atoi(rawMetricsCheckInterval)
		if err != nil {
			glog.Warningf(
				"There was an error getting custom metrics check interval value "+
					"in %s %s, falling back to the default value of %d "+
					"seconds; error: %s",
				kind,
				name,
				z.cfg.MetricsCheckInterval,
				err,
			)
			metricsCheckInterval = z.cfg.MetricsCheckInterval
		}
	}
	if metricsCheckInterval <= 0 {
		glog.Warningf(
			"Invalid custom metrics check interval value %d in %s %s,"+
				" falling back to the default value of %d seconds",
			metricsCheckInterval,
			kind,
			name,
			z.cfg.MetricsCheckInterval,
		)
		metricsCheckInterval = z.cfg.MetricsCheckInterval
	}
	return time.Duration(metricsCheckInterval) * time.Second
}

func getKey(kind, namespace, name string) string {
	return fmt.Sprintf("%s:%s/%s", kind, namespace, name)
}
