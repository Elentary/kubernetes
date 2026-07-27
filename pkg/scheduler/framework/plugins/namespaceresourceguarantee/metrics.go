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

	registerMetricsOnce sync.Once
)

func registerMetrics() {
	registerMetricsOnce.Do(func() {
		schedmetrics.RegisterMetrics(
			namespaceResourceGuaranteeQuota,
			namespaceResourceGuaranteeProtectedPriorityClassInfo,
		)
	})
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
