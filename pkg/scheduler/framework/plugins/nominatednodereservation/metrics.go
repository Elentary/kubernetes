/*
Copyright 2026 The Kubernetes Authors.

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

package nominatednodereservation

import (
	"sync"

	"k8s.io/component-base/metrics"
	schedmetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	transitionActionReserved = "reserved"
	transitionActionReleased = "released"
	transitionActionCleaned  = "cleaned"

	transitionReasonPreemptionStarted  = "preemption_started"
	transitionReasonHolderBound        = "holder_bound"
	transitionReasonNominationCleared  = "nomination_cleared"
	transitionReasonNoCandidate        = "no_candidate"
	transitionReasonHolderNotFound     = "holder_not_found"
	transitionReasonHolderUIDChanged   = "holder_uid_changed"
	transitionReasonHolderTerminating  = "holder_terminating"
	transitionReasonHolderAlreadyBound = "holder_already_bound"
	transitionReasonNominationMoved    = "nomination_moved"
)

var (
	nominatedNodeReservationsActive = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "nominated_node_reservations_active",
			Help:           "Number of active process-local nominated-node reservations.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	nominatedNodeReservationTransitions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "nominated_node_reservation_transitions_total",
			Help:           "Number of nominated-node reservation lifecycle transitions.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"action", "reason"},
	)

	nominatedNodeReservationBlockedPods = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "nominated_node_reservation_blocked_pods_total",
			Help:           "Number of scheduling attempts blocked to protect a nominated-node reservation.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile"},
	)

	registerMetricsOnce sync.Once
)

// RegisterMetrics registers NominatedNodeReservation metrics once per process.
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		schedmetrics.RegisterMetrics(
			nominatedNodeReservationsActive,
			nominatedNodeReservationTransitions,
			nominatedNodeReservationBlockedPods,
		)
	})
}

// RecordTransition records one bounded reservation lifecycle transition.
func RecordTransition(action, reason string) {
	nominatedNodeReservationTransitions.WithLabelValues(action, reason).Inc()
}

func recordBlockedPod(profile string) {
	nominatedNodeReservationBlockedPods.WithLabelValues(profile).Inc()
}

func resetMetricsForTest() {
	nominatedNodeReservationsActive.Set(0)
	nominatedNodeReservationTransitions.Reset()
	nominatedNodeReservationBlockedPods.Reset()
}
