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
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

// NamespaceResourceGuarantee enforces per-namespace protected resource guarantees.
type NamespaceResourceGuarantee struct {
	handle             framework.Handle
	args               config.NamespaceResourceGuaranteeArgs
	configuredResource []v1.ResourceName
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
		handle:             handle,
		args:               *args,
		configuredResource: configuredResources(args.NamespaceGuarantees),
	}, nil
}

// PreFilter checks whether a protected pod would exceed any namespace resource guarantee.
func (pl *NamespaceResourceGuarantee) PreFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	if !pl.isProtectedPod(pod) || len(pl.configuredResource) == 0 {
		return nil, nil
	}

	requested := pl.protectedPodRequests(pod)
	if len(requested) == 0 {
		return nil, nil
	}

	currentUsage, err := pl.namespaceProtectedUsage(pod.Namespace)
	if err != nil {
		return nil, framework.AsStatus(err)
	}

	for _, resourceName := range pl.configuredResource {
		resourceRequested := requested[resourceName]
		if resourceRequested == 0 {
			continue
		}
		resourceUsage := currentUsage[resourceName]
		resourceGuarantee := pl.namespaceGuaranteeValue(pod.Namespace, resourceName)
		if resourceUsage+resourceRequested > resourceGuarantee {
			return nil, framework.NewStatus(
				framework.UnschedulableAndUnresolvable,
				fmt.Sprintf(
					"namespace %q protected resource guarantee exceeded: resource=%q guarantee=%d current=%d requested=%d",
					pod.Namespace,
					resourceName,
					resourceGuarantee,
					resourceUsage,
					resourceRequested,
				),
			)
		}
	}

	return nil, nil
}

// PreFilterExtensions returns nil because this plugin maintains no incremental state.
func (pl *NamespaceResourceGuarantee) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// EventsToRegister returns the pod events that can reduce protected namespace resource usage.
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
		if pl.requestDecreased(originalPod, modifiedPod) {
			logger.V(5).Info("protected pod scaled down and may now fit under the namespace resource guarantee", "pod", klog.KObj(pod))
			return framework.Queue, nil
		}
		logger.V(5).Info("protected pod update did not reduce its relevant resource request", "pod", klog.KObj(pod))
		return framework.QueueSkip, nil
	}

	if pl.namespaceUsageDecreased(originalPod, modifiedPod, pod.Namespace) {
		logger.V(5).Info("namespace protected resource usage decreased and may unblock scheduling", "pod", klog.KObj(pod))
		return framework.Queue, nil
	}

	logger.V(5).Info("pod change did not reduce relevant namespace protected resource usage", "pod", klog.KObj(pod))
	return framework.QueueSkip, nil
}

func (pl *NamespaceResourceGuarantee) namespaceProtectedUsage(namespace string) (map[v1.ResourceName]int64, error) {
	usage := make(map[v1.ResourceName]int64, len(pl.configuredResource))
	for _, resourceName := range pl.configuredResource {
		usage[resourceName] = 0
	}

	sharedLister := pl.handle.SnapshotSharedLister()
	if sharedLister == nil {
		return nil, fmt.Errorf("snapshot shared lister is not available")
	}

	nodeInfos, err := sharedLister.NodeInfos().List()
	if err != nil {
		return nil, err
	}

	for _, nodeInfo := range nodeInfos {
		if nodeInfo == nil {
			continue
		}

		for _, podInfo := range nodeInfo.Pods {
			for _, resourceName := range pl.configuredResource {
				usage[resourceName] += pl.scheduledProtectedResourceRequest(podInfo.Pod, namespace, resourceName)
			}
		}
	}

	return usage, nil
}

func (pl *NamespaceResourceGuarantee) scheduledProtectedResourceRequest(pod *v1.Pod, namespace string, resourceName v1.ResourceName) int64 {
	if pod == nil || pod.Spec.NodeName == "" || pod.Namespace != namespace {
		return 0
	}
	return pl.protectedResourceRequest(pod, resourceName)
}

func (pl *NamespaceResourceGuarantee) protectedResourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {
	if !pl.isProtectedPod(pod) {
		return 0
	}
	return pl.resourceRequest(pod, resourceName)
}

func (pl *NamespaceResourceGuarantee) isProtectedPod(pod *v1.Pod) bool {
	return pod != nil && pod.Spec.PriorityClassName == pl.args.ProtectedPriorityClassName
}

func (pl *NamespaceResourceGuarantee) protectedPodRequests(pod *v1.Pod) map[v1.ResourceName]int64 {
	requested := make(map[v1.ResourceName]int64, len(pl.configuredResource))
	for _, resourceName := range pl.configuredResource {
		value := pl.protectedResourceRequest(pod, resourceName)
		if value > 0 {
			requested[resourceName] = value
		}
	}
	return requested
}

func (pl *NamespaceResourceGuarantee) requestDecreased(originalPod, modifiedPod *v1.Pod) bool {
	for _, resourceName := range pl.configuredResource {
		if pl.protectedResourceRequest(modifiedPod, resourceName) < pl.protectedResourceRequest(originalPod, resourceName) {
			return true
		}
	}
	return false
}

func (pl *NamespaceResourceGuarantee) namespaceUsageDecreased(originalPod, modifiedPod *v1.Pod, namespace string) bool {
	for _, resourceName := range pl.configuredResource {
		if pl.scheduledProtectedResourceRequest(originalPod, namespace, resourceName) > pl.scheduledProtectedResourceRequest(modifiedPod, namespace, resourceName) {
			return true
		}
	}
	return false
}

func (pl *NamespaceResourceGuarantee) namespaceGuaranteeValue(namespace string, resourceName v1.ResourceName) int64 {
	guaranteeByNamespace := pl.args.NamespaceGuarantees[namespace]
	return quantityValue(guaranteeByNamespace[resourceName], resourceName)
}

func (pl *NamespaceResourceGuarantee) resourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {
	if pod == nil {
		return 0
	}

	requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
		UseStatusResources:    utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		SkipPodLevelResources: !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	})

	return quantityValue(requests[resourceName], resourceName)
}

func quantityValue(quantity resource.Quantity, resourceName v1.ResourceName) int64 {
	switch resourceName {
	case v1.ResourceCPU:
		return quantity.MilliValue()
	default:
		return quantity.Value()
	}
}

func configuredResources(namespaceGuarantees map[string]v1.ResourceList) []v1.ResourceName {
	resourceSet := map[v1.ResourceName]struct{}{}
	for _, namespaceResources := range namespaceGuarantees {
		for resourceName := range namespaceResources {
			resourceSet[resourceName] = struct{}{}
		}
	}

	resources := make([]v1.ResourceName, 0, len(resourceSet))
	for resourceName := range resourceSet {
		resources = append(resources, resourceName)
	}
	sort.Slice(resources, func(i, j int) bool {
		return resources[i] < resources[j]
	})
	return resources
}
