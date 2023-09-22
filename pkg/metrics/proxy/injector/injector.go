package injector

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/glog"
	admissionsv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	"github.com/dailymotion-oss/osiris/pkg/healthz"
	"github.com/dailymotion-oss/osiris/pkg/kubernetes"
)

const port = 5000

type Injector interface {
	Run(ctx context.Context)
}

type injector struct {
	config       Config
	deserializer runtime.Decoder
	srv          *http.Server
}

func NewInjector(config Config) Injector {
	mux := http.NewServeMux()

	i := &injector{
		config: config,
		deserializer: serializer.NewCodecFactory(
			runtime.NewScheme(),
		).UniversalDeserializer(),
		srv: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
	}

	mux.HandleFunc("/mutate", i.handleRequest)
	mux.HandleFunc("/healthz", healthz.HandleHealthCheckRequest)

	return i
}

func (i *injector) Run(ctx context.Context) {
	doneCh := make(chan struct{})

	go func() {
		select {
		case <-ctx.Done(): // Context was canceled or expired
			glog.Info("Proxy injector is shutting down")
			// Allow up to five seconds for requests in progress to be completed
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Second*5,
			)
			defer cancel()
			i.srv.Shutdown(shutdownCtx) // nolint: errcheck
		case <-doneCh: // The server shut down on its own, perhaps due to an error
		}
	}()

	glog.Infof(
		"Proxy injector is listening on %s, patching Osiris-enabled pods",
		i.srv.Addr,
	)
	err := i.srv.ListenAndServeTLS(i.config.TLSCertFile, i.config.TLSKeyFile)
	if err != http.ErrServerClosed {
		glog.Errorf("Proxy injector error: %s", err)
	}
	close(doneCh)
}

func (i *injector) handleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var body []byte
	if r.Body != nil {
		if data, err := ioutil.ReadAll(r.Body); err == nil {
			body = data
		}
	}
	if len(body) == 0 {
		glog.Error("empty body")
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		glog.Errorf("Content-Type=%s, expect application/json", contentType)
		http.Error(
			w,
			"invalid Content-Type, expect `application/json`",
			http.StatusUnsupportedMediaType,
		)
		return
	}

	var admissionResponse *admissionsv1.AdmissionResponse
	var patchOps []kubernetes.PatchOperation
	var err error
	ar := admissionsv1.AdmissionReview{}
	if _, _, err = i.deserializer.Decode(body, nil, &ar); err != nil {
		glog.Errorf("Can't decode body: %v", err)
	} else {
		switch ar.Request.Kind.Kind {
		case "Pod":
			patchOps, err = i.getPodPatchOperations(&ar)
		default:
			err = fmt.Errorf("Invalid kind for review: %s", ar.Kind)
			glog.Error(err)
		}
	}

	if err != nil {
		admissionResponse = &admissionsv1.AdmissionResponse{
			UID: ar.Request.UID,
			Result: &metav1.Status{
				Message: err.Error(),
			},
		}
	} else if len(patchOps) == 0 {
		admissionResponse = &admissionsv1.AdmissionResponse{
			UID:     ar.Request.UID,
			Allowed: true,
		}
	} else {
		var patchBytes []byte
		patchBytes, err = json.Marshal(patchOps)
		if err != nil {
			admissionResponse = &admissionsv1.AdmissionResponse{
				UID: ar.Request.UID,
				Result: &metav1.Status{
					Message: err.Error(),
				},
			}
		} else {
			glog.Infof("AdmissionResponse: patch=%v\n", string(patchBytes))
			admissionResponse = &admissionsv1.AdmissionResponse{
				UID:     ar.Request.UID,
				Allowed: true,
				Patch:   patchBytes,
				PatchType: func() *admissionsv1.PatchType {
					pt := admissionsv1.PatchTypeJSONPatch
					return &pt
				}(),
			}
		}
	}

	admissionReview := admissionsv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: admissionResponse,
	}

	resp, err := json.Marshal(admissionReview)
	if err != nil {
		glog.Errorf("Can't encode response: %v", err)
		http.Error(
			w,
			fmt.Sprintf("could not encode response: %v", err),
			http.StatusInternalServerError,
		)
	}
	glog.Infof("Ready to write response ...")
	if _, err := w.Write(resp); err != nil {
		glog.Errorf("Can't write response: %v", err)
		http.Error(
			w,
			fmt.Sprintf("could not write response: %v", err),
			http.StatusInternalServerError,
		)
	}
}
