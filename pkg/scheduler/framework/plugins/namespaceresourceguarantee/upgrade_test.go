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
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	fwkapi "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
)

// Quota accounting and queue hints must use effective, not only desired, requests
// now that in-place resize is enabled by default in Kubernetes 1.33.
func TestResizeAccountingAndQueueHint(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	pl := &NamespaceResourceGuarantee{args: config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		NamespaceGuarantees:        map[string]v1.ResourceList{"team-a": {v1.ResourceCPU: resource.MustParse("4")}},
	}, configuredResource: []v1.ResourceName{v1.ResourceCPU}}
	pending := makePodWithRequests("pending", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	old := makePodWithRequests("resizing", "team-a", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	old.Status.ContainerStatuses = []v1.ContainerStatus{{Name: old.Spec.Containers[0].Name,
		Resources:          &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")}},
		AllocatedResources: v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")},
	}}
	if got := pl.resourceRequest(old, v1.ResourceCPU); got != 3000 {
		t.Fatalf("in-progress downsize usage=%d, want 3000", got)
	}
	unchanged := old.DeepCopy()
	unchanged.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("500m")
	if hint, err := pl.isSchedulableAfterPodChange(klog.Background(), pending, old, unchanged); err != nil || hint != fwkapi.QueueSkip {
		t.Fatalf("premature requeue: %v %v", hint, err)
	}
	completed := old.DeepCopy()
	completed.Status.ContainerStatuses[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1")
	completed.Status.ContainerStatuses[0].AllocatedResources[v1.ResourceCPU] = resource.MustParse("1")
	if got := pl.resourceRequest(completed, v1.ResourceCPU); got != 1000 {
		t.Fatalf("completed usage=%d, want 1000", got)
	}
	if hint, err := pl.isSchedulableAfterPodChange(klog.Background(), pending, old, completed); err != nil || hint != fwkapi.Queue {
		t.Fatalf("missing status-only requeue: %v %v", hint, err)
	}
	foundScaleDown := false
	for _, event := range framework.PodSchedulingPropertiesChange(completed, old) {
		if event.ActionType&fwkapi.UpdatePodScaleDown != 0 {
			foundScaleDown = true
		}
	}
	if !foundScaleDown {
		t.Fatal("status-only resize does not produce registered scale-down event")
	}
	missing := old.DeepCopy()
	missing.Status.ContainerStatuses[0].Resources = nil
	if got := pl.resourceRequest(missing, v1.ResourceCPU); got != 1000 {
		t.Fatalf("missing status resources usage=%d, want 1000", got)
	}
}

// Score must consume the supplied NodeInfo; a second snapshot lookup is obsolete.
func TestScoreUsesSuppliedNodeInfo(t *testing.T) {
	pl := &NamespaceResourceGuarantee{args: config.NamespaceResourceGuaranteeArgs{ProtectedPriorityClassName: "protected"}, configuredResource: []v1.ResourceName{protectedGPUResource}}
	incoming := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{protectedGPUResource: "1"})
	existing := makePodWithRequests("existing", "team-a", "protected", "node-a", map[v1.ResourceName]string{protectedGPUResource: "3"})
	node := framework.NewNodeInfo(existing)
	n := makeNode("node-a")
	n.Status.Allocatable[protectedGPUResource] = resource.MustParse("8")
	node.SetNode(n)
	score, status := pl.Score(context.Background(), framework.NewCycleState(), incoming, node)
	if !status.IsSuccess() || score != 50 {
		t.Fatalf("score=%d status=%v, want 50", score, status)
	}
}

// In 1.34 PodLevelResources is enabled by default. Both incoming requests and
// bound usage must include spec.resources, including pods with empty containers' requests.
func TestPodLevelQuotaAccounting(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, true)
	existing := makePodWithRequests("existing", "team-a", "protected", "node-a", nil)
	existing.Spec.Resources = &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}
	incoming := makePodWithRequests("incoming", "team-a", "protected", "", nil)
	incoming.Spec.Resources = &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}
	ctx := context.Background()
	pl := plugintesting.SetupPlugin(ctx, t, New, &config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		NamespaceGuarantees:        map[string]v1.ResourceList{"team-a": {v1.ResourceCPU: resource.MustParse("3")}},
	}, internalcache.NewSnapshot([]*v1.Pod{existing}, []*v1.Node{makeNode("node-a")})).(*NamespaceResourceGuarantee)
	if _, status := pl.PreFilter(ctx, framework.NewCycleState(), incoming, nil); status.Code() != fwkapi.UnschedulableAndUnresolvable {
		t.Fatalf("pod-level request must exceed shared guarantee: %v", status)
	}
	incoming.Spec.Resources.Requests[v1.ResourceCPU] = resource.MustParse("1")
	if _, status := pl.PreFilter(ctx, framework.NewCycleState(), incoming, nil); !status.IsSuccess() {
		t.Fatalf("pod-level request at guarantee must fit: %v", status)
	}
}

// Pod-level resize has separate status/allocated fields and a separate gate in
// 1.35. Keep quota usage and scale-down hints consistent with NodeResourcesFit.
func TestPodLevelResizeAccountingAndQueueHint(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, enabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodLevelResourcesVerticalScaling, enabled)
			pl := &NamespaceResourceGuarantee{args: config.NamespaceResourceGuaranteeArgs{
				ProtectedPriorityClassName: "protected",
			}, configuredResource: []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory}}
			pending := makePodWithRequests("pending", "team-a", "protected", "", nil)
			old := makePodWithRequests("resizing", "team-a", "protected", "node-a", nil)
			old.Spec.Resources = &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("1"), v1.ResourceMemory: resource.MustParse("1Gi"),
			}}
			old.Status.Resources = &v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"), v1.ResourceMemory: resource.MustParse("3Gi"),
			}}
			old.Status.AllocatedResources = old.Status.Resources.Requests.DeepCopy()
			wantCPU, wantMemory := int64(1000), int64(1<<30)
			if enabled {
				wantCPU, wantMemory = 3000, 3<<30
			}
			if got := pl.resourceRequest(old, v1.ResourceCPU); got != wantCPU {
				t.Fatalf("pending downsize CPU=%d, want %d", got, wantCPU)
			}
			if got := pl.resourceRequest(old, v1.ResourceMemory); got != wantMemory {
				t.Fatalf("pending downsize memory=%d, want %d", got, wantMemory)
			}
			completed := old.DeepCopy()
			completed.Status.Resources = completed.Spec.Resources.DeepCopy()
			completed.Status.AllocatedResources = completed.Spec.Resources.Requests.DeepCopy()
			if got := pl.resourceRequest(completed, v1.ResourceCPU); got != 1000 {
				t.Fatalf("completed CPU=%d, want 1000", got)
			}
			wantHint := fwkapi.QueueSkip
			if enabled {
				wantHint = fwkapi.Queue
			}
			if hint, err := pl.isSchedulableAfterPodChange(klog.Background(), pending, old, completed); err != nil || hint != wantHint {
				t.Fatalf("status-only hint=%v err=%v, want %v", hint, err, wantHint)
			}
			if enabled {
				found := false
				for _, event := range framework.PodSchedulingPropertiesChange(completed, old) {
					found = found || event.ActionType&fwkapi.UpdatePodScaleDown != 0
				}
				if !found {
					t.Fatal("pod-level status downsize did not produce scale-down event")
				}
			}
		})
	}
}
