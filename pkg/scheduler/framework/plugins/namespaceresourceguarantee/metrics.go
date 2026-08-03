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

package namespaceresourceguarantee

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/metrics"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	schedmetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
)

const (
	preFilterResultAllowed       = "allowed"
	preFilterResultQuotaExceeded = "quota_exceeded"
	preFilterResultError         = "error"

	preemptionOutcomeIneligible  = "ineligible"
	preemptionOutcomeStarted     = "started"
	preemptionOutcomeWaiting     = "waiting"
	preemptionOutcomeNotHelpful  = "not_helpful"
	preemptionOutcomeNoCandidate = "no_candidate"
	preemptionOutcomeError       = "error"
)

var (
	namespaceResourceGuaranteeQuota = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_quota",
			Help:           "Configured per-namespace protected resource quota for NamespaceResourceGuarantee plugin.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "namespace", "resource", "unit"},
	)

	namespaceResourceGuaranteeProtectedPriorityClassInfo = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_protected_priority_class_info",
			Help:           "Information about the configured protected priority class for NamespaceResourceGuarantee plugin.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "priority_class"},
	)

	namespaceResourceGuaranteePreFilterDecisions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_prefilter_decisions_total",
			Help:           "Number of protected-pod namespace resource guarantee PreFilter decisions.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "namespace", "tier", "result"},
	)

	namespaceResourceGuaranteeQuotaExceeded = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_quota_exceeded_total",
			Help:           "Number of protected-pod PreFilter decisions rejected because a namespace resource guarantee would be exceeded.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "namespace", "tier", "resource"},
	)

	namespaceResourceGuaranteePreemptionOutcomes = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_preemption_outcomes_total",
			Help:           "Number of NamespaceResourceGuarantee PostFilter outcomes.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "namespace", "tier", "outcome"},
	)

	namespaceResourceGuaranteeVictimDeletions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      schedmetrics.SchedulerSubsystem,
			Name:           "namespace_resource_guarantee_victim_deletions_total",
			Help:           "Number of successful preemption victim deletion requests initiated by NamespaceResourceGuarantee.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"profile", "namespace", "tier"},
	)

	registerMetricsOnce sync.Once
)

func registerMetrics() {
	registerMetricsOnce.Do(func() {
		schedmetrics.RegisterMetrics(
			namespaceResourceGuaranteeQuota,
			namespaceResourceGuaranteeProtectedPriorityClassInfo,
			namespaceResourceGuaranteePreFilterDecisions,
			namespaceResourceGuaranteeQuotaExceeded,
			namespaceResourceGuaranteePreemptionOutcomes,
			namespaceResourceGuaranteeVictimDeletions,
		)
	})
}

func resetMetricsForTest() {
	namespaceResourceGuaranteeQuota.Reset()
	namespaceResourceGuaranteeProtectedPriorityClassInfo.Reset()
	namespaceResourceGuaranteePreFilterDecisions.Reset()
	namespaceResourceGuaranteeQuotaExceeded.Reset()
	namespaceResourceGuaranteePreemptionOutcomes.Reset()
	namespaceResourceGuaranteeVictimDeletions.Reset()
}

func (pl *NamespaceResourceGuarantee) recordPreFilterDecision(pod *v1.Pod, tier protectedTier, result string) {
	namespaceResourceGuaranteePreFilterDecisions.WithLabelValues(
		pl.profile,
		pod.Namespace,
		string(tier),
		result,
	).Inc()
}

func (pl *NamespaceResourceGuarantee) recordQuotaExceeded(pod *v1.Pod, tier protectedTier, resourceName v1.ResourceName) {
	namespaceResourceGuaranteeQuotaExceeded.WithLabelValues(
		pl.profile,
		pod.Namespace,
		string(tier),
		string(resourceName),
	).Inc()
}

func (pl *NamespaceResourceGuarantee) recordPreemptionOutcome(pod *v1.Pod, tier, outcome string) {
	namespaceResourceGuaranteePreemptionOutcomes.WithLabelValues(
		pl.profile,
		pod.Namespace,
		tier,
		outcome,
	).Inc()
}

func preemptionOutcomeForEventReason(reason string) string {
	switch reason {
	case preemptionStartedReason:
		return preemptionOutcomeStarted
	case preemptionWaitingReason:
		return preemptionOutcomeWaiting
	case preemptionNotHelpfulReason:
		return preemptionOutcomeNotHelpful
	case preemptionNoCandidateReason:
		return preemptionOutcomeNoCandidate
	default:
		return preemptionOutcomeError
	}
}

func (pl *NamespaceResourceGuarantee) recordConfiguredQuotaMetrics(profile string) {
	for namespace, resourceList := range pl.args.NamespaceGuarantees {
		for resourceName, quantity := range resourceList {
			namespaceResourceGuaranteeQuota.WithLabelValues(
				profile,
				namespace,
				string(resourceName),
				resourceUnit(resourceName),
			).Set(float64(quantityValue(quantity, resourceName)))
		}
	}
	namespaceResourceGuaranteeProtectedPriorityClassInfo.WithLabelValues(
		profile,
		pl.args.ProtectedPriorityClassName,
	).Set(1)
	if len(pl.args.SemiProtectedPriorityClassName) > 0 {
		namespaceResourceGuaranteeProtectedPriorityClassInfo.WithLabelValues(
			profile,
			pl.args.SemiProtectedPriorityClassName,
		).Set(1)
	}
}

func resourceUnit(resourceName v1.ResourceName) string {
	switch resourceName {
	case v1.ResourceCPU:
		return "millicore"
	case v1.ResourceMemory:
		return "byte"
	default:
		return "unit"
	}
}

func profileName(handle framework.Handle) string {
	if handle == nil {
		return ""
	}
	if profileAwareHandle, ok := handle.(interface{ ProfileName() string }); ok {
		return profileAwareHandle.ProfileName()
	}
	return ""
}
