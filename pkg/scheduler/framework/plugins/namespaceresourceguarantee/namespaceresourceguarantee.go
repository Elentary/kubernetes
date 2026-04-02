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
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.NamespaceResourceGuarantee
)

// NamespaceResourceGuarantee enforces per-namespace protected GPU guarantees.
type NamespaceResourceGuarantee struct {
	handle framework.Handle
	args   config.NamespaceResourceGuaranteeArgs
}

var _ framework.PreFilterPlugin = &NamespaceResourceGuarantee{}
var _ framework.EnqueueExtensions = &NamespaceResourceGuarantee{}

// Name returns the plugin name.
func (pl *NamespaceResourceGuarantee) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.NamespaceResourceGuaranteeArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *NamespaceResourceGuaranteeArgs", obj)
	}
	if err := validation.ValidateNamespaceResourceGuaranteeArgs(nil, args); err != nil {
		return nil, err
	}

	return &NamespaceResourceGuarantee{
		handle: handle,
		args:   *args,
	}, nil
}

// PreFilter checks whether a protected GPU pod would exceed its namespace guarantee.
func (pl *NamespaceResourceGuarantee) PreFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	if !pl.isProtectedPod(pod) {
		return nil, nil
	}

	requested := pl.protectedGPURequest(pod)
	if requested == 0 {
		return nil, nil
	}

	currentUsage, err := pl.namespaceProtectedGPUUsage(pod.Namespace)
	if err != nil {
		return nil, framework.AsStatus(err)
	}

	guarantee := pl.args.NamespaceGuarantees[pod.Namespace]
	if currentUsage+requested > guarantee {
		return nil, framework.NewStatus(
			framework.UnschedulableAndUnresolvable,
			fmt.Sprintf(
				"namespace %q protected GPU guarantee exceeded: guarantee=%d current=%d requested=%d",
				pod.Namespace,
				guarantee,
				currentUsage,
				requested,
			),
		)
	}

	return nil, nil
}

// PreFilterExtensions returns nil because this plugin maintains no incremental state.
func (pl *NamespaceResourceGuarantee) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// EventsToRegister returns the pod events that can reduce protected GPU usage.
func (pl *NamespaceResourceGuarantee) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{
		{
			Event:          framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete | framework.UpdatePodScaleDown},
			QueueingHintFn: pl.isSchedulableAfterPodChange,
		},
	}, nil
}

func (pl *NamespaceResourceGuarantee) isSchedulableAfterPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalPod, modifiedPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return framework.Queue, err
	}

	// The unschedulable pod itself may become schedulable when it scales down.
	if modifiedPod != nil && modifiedPod.UID == pod.UID {
		if pl.protectedGPURequest(modifiedPod) < pl.protectedGPURequest(originalPod) {
			logger.V(5).Info("protected pod scaled down and may now fit under the namespace GPU guarantee", "pod", klog.KObj(pod))
			return framework.Queue, nil
		}
		logger.V(5).Info("protected pod update did not reduce its GPU request", "pod", klog.KObj(pod))
		return framework.QueueSkip, nil
	}

	if pl.scheduledProtectedGPURequest(originalPod, pod.Namespace) > pl.scheduledProtectedGPURequest(modifiedPod, pod.Namespace) {
		logger.V(5).Info("namespace protected GPU usage decreased and may unblock scheduling", "pod", klog.KObj(pod))
		return framework.Queue, nil
	}

	logger.V(5).Info("pod change did not reduce relevant namespace protected GPU usage", "pod", klog.KObj(pod))
	return framework.QueueSkip, nil
}

func (pl *NamespaceResourceGuarantee) namespaceProtectedGPUUsage(namespace string) (int64, error) {
	sharedLister := pl.handle.SnapshotSharedLister()
	if sharedLister == nil {
		return 0, fmt.Errorf("snapshot shared lister is not available")
	}

	nodeInfos, err := sharedLister.NodeInfos().List()
	if err != nil {
		return 0, err
	}

	var usage int64
	resourceName := v1.ResourceName(pl.args.GPUResourceName)
	for _, nodeInfo := range nodeInfos {
		if nodeInfo == nil || nodeInfo.Node() == nil || nodeInfo.Allocatable == nil {
			continue
		}
		if nodeInfo.Allocatable.ScalarResources[resourceName] == 0 {
			continue
		}
		for _, podInfo := range nodeInfo.Pods {
			usage += pl.scheduledProtectedGPURequest(podInfo.Pod, namespace)
		}
	}

	return usage, nil
}

func (pl *NamespaceResourceGuarantee) scheduledProtectedGPURequest(pod *v1.Pod, namespace string) int64 {
	if pod == nil || pod.Spec.NodeName == "" || pod.Namespace != namespace {
		return 0
	}
	return pl.protectedGPURequest(pod)
}

func (pl *NamespaceResourceGuarantee) protectedGPURequest(pod *v1.Pod) int64 {
	if !pl.isProtectedPod(pod) {
		return 0
	}
	return pl.gpuRequest(pod)
}

func (pl *NamespaceResourceGuarantee) isProtectedPod(pod *v1.Pod) bool {
	return pod != nil && pod.Spec.PriorityClassName == pl.args.ProtectedPriorityClassName
}

func (pl *NamespaceResourceGuarantee) gpuRequest(pod *v1.Pod) int64 {
	if pod == nil {
		return 0
	}

	requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
		UseStatusResources:    utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		SkipPodLevelResources: !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	})

	quantity := requests[v1.ResourceName(pl.args.GPUResourceName)]
	return quantity.Value()
}
