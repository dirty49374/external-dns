/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"

	"sigs.k8s.io/external-dns/endpoint"
	"sigs.k8s.io/external-dns/plan"
	"sigs.k8s.io/external-dns/provider"
	"sigs.k8s.io/external-dns/registry"
	"sigs.k8s.io/external-dns/source"
)

var (
	registryErrorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "external_dns",
			Subsystem: "registry",
			Name:      "errors_total",
			Help:      "Number of Registry errors.",
		},
	)
	sourceErrorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "external_dns",
			Subsystem: "source",
			Name:      "errors_total",
			Help:      "Number of Source errors.",
		},
	)
	sourceEndpointsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "external_dns",
			Subsystem: "source",
			Name:      "endpoints_total",
			Help:      "Number of Endpoints in all sources",
		},
	)
	registryEndpointsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "external_dns",
			Subsystem: "registry",
			Name:      "endpoints_total",
			Help:      "Number of Endpoints in the registry",
		},
	)
	lastSyncTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "external_dns",
			Subsystem: "controller",
			Name:      "last_sync_timestamp_seconds",
			Help:      "Timestamp of last successful sync with the DNS provider",
		},
	)
	deprecatedRegistryErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: "registry",
			Name:      "errors_total",
			Help:      "Number of Registry errors.",
		},
	)
	deprecatedSourceErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Subsystem: "source",
			Name:      "errors_total",
			Help:      "Number of Source errors.",
		},
	)
)

func init() {
	prometheus.MustRegister(registryErrorsTotal)
	prometheus.MustRegister(sourceErrorsTotal)
	prometheus.MustRegister(sourceEndpointsTotal)
	prometheus.MustRegister(registryEndpointsTotal)
	prometheus.MustRegister(lastSyncTimestamp)
	prometheus.MustRegister(deprecatedRegistryErrors)
	prometheus.MustRegister(deprecatedSourceErrors)
}

// Controller is responsible for orchestrating the different components.
// It works in the following way:
// * Ask the DNS provider for current list of endpoints.
// * Ask the Source for the desired list of endpoints.
// * Take both lists and calculate a Plan to move current towards desired state.
// * Tell the DNS provider to apply the changes calucated by the Plan.
type Controller struct {
	Source   source.Source
	Registry registry.Registry
	// The policy that defines which changes to DNS records are allowed
	Policy plan.Policy
	// The interval between individual synchronizations
	Interval time.Duration
	// The interval when error occurred
	IntervalError time.Duration
	// The jitter duration of synchronizations interval
	IntervalJitter time.Duration
	// The DomainFilter defines which DNS records to keep or exclude
	DomainFilter endpoint.DomainFilter
	// List of desired records that successfully synced
	lastDesiredRecords []*endpoint.Endpoint
	// mutex
	mutex sync.Mutex
	// is running
	running bool
}

func (c *Controller) setRunning() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.running {
		return false
	}
	c.running = true
	return true
}

func (c *Controller) clearRunning() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.running = false
}

// RunOnce runs a single iteration of a reconciliation loop.
func (c *Controller) RunOnce(ctx context.Context) error {
	log.Info("XXX: RunOnce() started")
	defer log.Info("XXX: RunOnce() finished")

	if !c.setRunning() {
		return nil
	}

	defer c.clearRunning()

	records, err := c.Registry.Records(ctx)
	if err != nil {
		registryErrorsTotal.Inc()
		deprecatedRegistryErrors.Inc()
		return err
	}
	registryEndpointsTotal.Set(float64(len(records)))

	ctx = context.WithValue(ctx, provider.RecordsContextKey, records)

	endpoints, err := c.Source.Endpoints()
	if err != nil {
		sourceErrorsTotal.Inc()
		deprecatedSourceErrors.Inc()
		return err
	}
	sourceEndpointsTotal.Set(float64(len(endpoints)))

	plan := &plan.Plan{
		Policies:     []plan.Policy{c.Policy},
		Current:      records,
		Desired:      endpoints,
		DomainFilter: c.DomainFilter,
	}

	plan = plan.Calculate()

	err = c.Registry.ApplyChanges(ctx, plan.Changes)
	if err != nil {
		registryErrorsTotal.Inc()
		deprecatedRegistryErrors.Inc()
		return err
	}

	c.lastDesiredRecords = endpoints

	lastSyncTimestamp.SetToCurrentTime()
	return nil
}

func (c *Controller) TestAndRunOnce(ctx context.Context) error {
	if c.lastDesiredRecords == nil {
		log.Trace("XXX: test run denied - no lastrun")
		return nil
	}

	endpoints, err := c.Source.Endpoints()
	if err != nil {
		log.Trace("XXX: test run denied - source error")
		return err
	}

	if endpointsEquals(c.lastDesiredRecords, endpoints) {
		log.Trace("XXX: test run denied - same")
		return nil
	}

	return c.RunOnce(ctx)
}

func (c *Controller) randJitter() time.Duration {
	return time.Duration(rand.Int63n(int64(c.IntervalJitter)))
}

// Run runs RunOnce in a loop with a delay until stopChan receives a value.
func (c *Controller) Run(ctx context.Context, stopChan <-chan struct{}) {
	for {
		err := c.RunOnce(ctx)
		if err != nil {
			log.Error(err)
		}

		jitter := c.randJitter()
		interval := c.Interval + jitter
		if err != nil && c.IntervalError != 0 {
			interval = c.IntervalError + jitter
		}
		log.Info("XXX: sleep interval - ", interval)

		select {
		case <-time.After(interval):
		case <-stopChan:
			log.Info("Terminating main controller loop")
			return
		}
	}
}

func endpointsEquals(last []*endpoint.Endpoint, curr []*endpoint.Endpoint) bool {
	if len(last) != len(curr) {
		log.Tracef("Test: # of records changed (last=%d current=%d)", len(last), len(curr))
		return false
	}

	lastEndpoints := make(map[string]*endpoint.Endpoint, len(last))
	for _, endpoint := range last {
		lastEndpoints[endpoint.DNSName] = endpoint
	}

	for _, currEndpoint := range curr {
		lastEndpoint, ok := lastEndpoints[currEndpoint.DNSName]
		if !ok {
			log.Infof("Test: found new record (%s)", currEndpoint.DNSName)
			return false
		}

		if !endpointEquals(lastEndpoint, currEndpoint) {
			log.Infof("Test: found changed record (%s)", currEndpoint.DNSName)
			return false
		}
	}

	log.Infof("Test: not changed")
	return true
}

func endpointEquals(last *endpoint.Endpoint, curr *endpoint.Endpoint) bool {
	if last == curr {
		return true
	}

	if last.DNSName != curr.DNSName {
		return false
	}
	if !reflect.DeepEqual(last.Targets, curr.Targets) {
		return false
	}
	if last.RecordType != curr.RecordType {
		return false
	}
	if last.SetIdentifier != curr.SetIdentifier {
		return false
	}
	if last.RecordTTL != curr.RecordTTL {
		return false
	}
	if !reflect.DeepEqual(last.ProviderSpecific, curr.ProviderSpecific) {
		return false
	}

	return true
}
