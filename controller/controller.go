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
	// The DomainFilter defines which DNS records to keep or exclude
	DomainFilter endpoint.DomainFilter
	// The nextRunAt used for throttling and batching reconciliation
	nextRunAt time.Time
	// The nextCompareAt used for throttling comparing desigred records changes
	nextTestAt time.Time
	// The nextRunAtMux is for atomic updating of nextRunAt
	nextRunAtMux sync.Mutex
	// List of desired records that successfully synced
	lastDesiredRecords []*endpoint.Endpoint
}

// RunOnce runs a single iteration of a reconciliation loop.
func (c *Controller) RunOnce(ctx context.Context) error {
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

func (c *Controller) TestOnce(ctx context.Context) (bool, error) {
	if c.lastDesiredRecords == nil {
		return true, nil
	}

	endpoints, err := c.Source.Endpoints()
	if err != nil {
		return false, err
	}

	return !endpointsEquals(c.lastDesiredRecords, endpoints), nil
}

// MIN_INTERVAL is used as window for batching events
const MIN_INTERVAL = 5 * time.Second

func (c *Controller) ScheduleRunOnce(now time.Time) {
	c.nextRunAtMux.Lock()
	defer c.nextRunAtMux.Unlock()
	c.nextRunAt = now.Add(MIN_INTERVAL)
}

// ShouldRunOnce makes sure execution happens at most once per interval.
func (c *Controller) ShouldRunOnce(now time.Time) bool {
	c.nextRunAtMux.Lock()
	defer c.nextRunAtMux.Unlock()
	if now.Before(c.nextRunAt) {
		return false
	}
	c.nextRunAt = now.Add(c.Interval)
	return true
}

func (c *Controller) ScheduleTestOnce(now time.Time) {
	c.nextRunAtMux.Lock()
	defer c.nextRunAtMux.Unlock()
	c.nextTestAt = now.Add(MIN_INTERVAL)
}

func (c *Controller) ShouldTestOnce(now time.Time) bool {
	c.nextRunAtMux.Lock()
	defer c.nextRunAtMux.Unlock()
	if c.nextTestAt.IsZero() || now.Before(c.nextTestAt) {
		return false
	}
	c.nextTestAt = time.Time{}
	return true
}

// Run runs RunOnce in a loop with a delay until context is cancelled
func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if c.ShouldRunOnce(time.Now()) {
			if err := c.RunOnce(ctx); err != nil {
				log.Error(err)
			}
		} else if c.ShouldTestOnce(time.Now()) {
			if changed, err := c.TestOnce(ctx); err != nil {
				log.Error(err)
			} else if changed {
				c.ScheduleRunOnce(time.Now())
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
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
