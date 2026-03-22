/*
Copyright 2024 The Kubernetes Authors.

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

// Package namespacerguarantee implements a PreFilter scheduler plugin that
// enforces per-namespace guaranteed resource allocations for protected-priority
// workloads. Currently supports nvidia.com/gpu; CPU support is planned.
package namespacerguarantee

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

const (
	// Name is the plugin name used in the registry and configurations.
	Name = names.NamespaceResourceGuarantee

	// gpuResourceName is the extended resource name for NVIDIA GPUs.
	gpuResourceName = "nvidia.com/gpu"
)

// NamespaceResourceGuarantee enforces per-namespace guaranteed resource
// allocations for protected-priority workloads.
//
// Protected pods (identified by PriorityClass) may only be scheduled if the
// namespace's current protected GPU usage plus the pod's GPU request does not
// exceed the configured namespace guarantee. Normal pods are not gated and may
// use GPUs opportunistically.
//
// The plugin relies on the scheduler snapshot for pod accounting: only pods
// already bound to a node appear in the snapshot, so we count currently
// consuming capacity. Preemption of normal pods is handled by the built-in
// DefaultPreemption plugin via PriorityClass ordering.
type NamespaceResourceGuarantee struct {
	handle                     framework.Handle
	namespaceGPUGuarantees     map[string]int64 // namespace → max protected GPUs
	protectedPriorityClassName string
}

var _ framework.PreFilterPlugin = &NamespaceResourceGuarantee{}

// Name returns the plugin name.
func (n *NamespaceResourceGuarantee) Name() string { return Name }

// New instantiates the NamespaceResourceGuarantee plugin.
func New(_ context.Context, plArgs runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, ok := plArgs.(*config.NamespaceResourceGuaranteeArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type *config.NamespaceResourceGuaranteeArgs, got %T", plArgs)
	}
	if args.ProtectedPriorityClassName == "" {
		return nil, fmt.Errorf("NamespaceResourceGuaranteeArgs.ProtectedPriorityClassName must not be empty")
	}
	return &NamespaceResourceGuarantee{
		handle:                     h,
		namespaceGPUGuarantees:     args.NamespaceGPUGuarantees,
		protectedPriorityClassName: args.ProtectedPriorityClassName,
	}, nil
}

// PreFilter gates protected GPU pods against their namespace GPU guarantee.
// Normal pods and non-GPU protected pods pass through without any check.
func (n *NamespaceResourceGuarantee) PreFilter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// Only enforce the guarantee for protected-priority pods.
	if pod.Spec.PriorityClassName != n.protectedPriorityClassName {
		return nil, nil
	}

	requestedGPUs := podGPURequest(pod)
	if requestedGPUs == 0 {
		// Protected pod with no GPU request — no GPU guarantee to enforce.
		return nil, nil
	}

	guarantee, exists := n.namespaceGPUGuarantees[pod.Namespace]
	if !exists {
		return nil, framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("namespace %q has no GPU guarantee configured in NamespaceResourceGuarantee plugin", pod.Namespace))
	}

	usedGPUs, err := n.countProtectedGPUsInNamespace(pod.Namespace)
	if err != nil {
		return nil, framework.AsStatus(fmt.Errorf("counting protected GPU usage: %w", err))
	}

	if usedGPUs+requestedGPUs > guarantee {
		return nil, framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("namespace %q: protected GPU usage %d + requested %d exceeds guarantee %d",
				pod.Namespace, usedGPUs, requestedGPUs, guarantee))
	}

	return nil, nil
}

// PreFilterExtensions returns nil because this plugin does not need to track
// per-node pod add/remove events.
func (n *NamespaceResourceGuarantee) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// countProtectedGPUsInNamespace returns the total nvidia.com/gpu requested by
// all protected pods currently scheduled (bound to a node) in namespace.
func (n *NamespaceResourceGuarantee) countProtectedGPUsInNamespace(namespace string) (int64, error) {
	nodeInfoList, err := n.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, fmt.Errorf("listing nodes from snapshot: %w", err)
	}

	var total int64
	for _, nodeInfo := range nodeInfoList {
		for _, podInfo := range nodeInfo.Pods {
			p := podInfo.Pod
			if p.Namespace != namespace {
				continue
			}
			if p.Spec.PriorityClassName != n.protectedPriorityClassName {
				continue
			}
			total += podGPURequest(p)
		}
	}
	return total, nil
}

// podGPURequest returns the sum of nvidia.com/gpu requests across all
// init containers and regular containers in the pod.
func podGPURequest(pod *v1.Pod) int64 {
	var total int64
	for _, c := range pod.Spec.InitContainers {
		if q, ok := c.Resources.Requests[gpuResourceName]; ok {
			total += q.Value()
		}
	}
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Requests[gpuResourceName]; ok {
			total += q.Value()
		}
	}
	return total
}
