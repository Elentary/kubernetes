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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
)

func TestPreFilter(t *testing.T) {
	tests := []struct {
		name         string
		args         config.NamespaceResourceGuaranteeArgs
		pod          *v1.Pod
		existingPods []*v1.Pod
		nodes        []*v1.Node
		wantCode     framework.Code
		wantMessage  string
	}{
		{
			name: "protected pod within guarantee passes",
			args: newArgs(map[string]int64{"team-a": 4}),
			pod:  makeGPUPod("incoming", "team-a", "protected", "", 2),
			existingPods: []*v1.Pod{
				makeGPUPod("running", "team-a", "protected", "node-a", 2),
			},
			nodes:    []*v1.Node{makeGPUNode("node-a", 8)},
			wantCode: framework.Success,
		},
		{
			name: "protected pod exceeding guarantee fails",
			args: newArgs(map[string]int64{"team-a": 3}),
			pod:  makeGPUPod("incoming", "team-a", "protected", "", 2),
			existingPods: []*v1.Pod{
				makeGPUPod("running", "team-a", "protected", "node-a", 2),
			},
			nodes:       []*v1.Node{makeGPUNode("node-a", 8)},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" protected GPU guarantee exceeded: guarantee=3 current=2 requested=2`,
		},
		{
			name: "missing namespace guarantee defaults to zero",
			args: newArgs(map[string]int64{"team-b": 4}),
			pod:  makeGPUPod("incoming", "team-a", "protected", "", 1),
			nodes: []*v1.Node{
				makeGPUNode("node-a", 8),
			},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" protected GPU guarantee exceeded: guarantee=0 current=0 requested=1`,
		},
		{
			name: "normal pod bypasses plugin",
			args: newArgs(map[string]int64{"team-a": 0}),
			pod:  makeGPUPod("incoming", "team-a", "normal", "", 8),
			nodes: []*v1.Node{
				makeGPUNode("node-a", 8),
			},
			wantCode: framework.Success,
		},
		{
			name: "protected cpu only pod bypasses cap accounting",
			args: newArgs(map[string]int64{"team-a": 0}),
			pod:  makeCPUPod("incoming", "team-a", "protected"),
			nodes: []*v1.Node{
				makeGPUNode("node-a", 8),
			},
			wantCode: framework.Success,
		},
		{
			name: "only same namespace protected gpu pods are counted",
			args: newArgs(map[string]int64{"team-a": 2}),
			pod:  makeGPUPod("incoming", "team-a", "protected", "", 1),
			existingPods: []*v1.Pod{
				makeGPUPod("other-ns", "team-b", "protected", "node-a", 8),
				makeGPUPod("normal", "team-a", "normal", "node-a", 8),
			},
			nodes:    []*v1.Node{makeGPUNode("node-a", 8)},
			wantCode: framework.Success,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			plugin := plugintesting.SetupPlugin(ctx, t, New, &tt.args, internalcache.NewSnapshot(tt.existingPods, tt.nodes)).(*NamespaceResourceGuarantee)
			_, status := plugin.PreFilter(ctx, framework.NewCycleState(), tt.pod)
			if status == nil {
				status = framework.NewStatus(framework.Success)
			}
			if status.Code() != tt.wantCode {
				t.Fatalf("unexpected status code: got %v, want %v", status.Code(), tt.wantCode)
			}
			if tt.wantMessage != "" && status.Message() != tt.wantMessage {
				t.Fatalf("unexpected status message: got %q, want %q", status.Message(), tt.wantMessage)
			}
		})
	}
}

func TestEventsToRegister(t *testing.T) {
	pl := &NamespaceResourceGuarantee{}
	_, ctx := ktesting.NewTestContext(t)
	events, err := pl.EventsToRegister(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].QueueingHintFn = nil
	}

	expected := []framework.ClusterEventWithHint{
		{Event: framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete | framework.UpdatePodScaleDown}},
	}
	if diff := cmp.Diff(expected, events, cmpopts.EquateComparable(framework.ClusterEvent{})); diff != "" {
		t.Fatalf("unexpected events (-want,+got):\n%s", diff)
	}
}

func TestIsSchedulableAfterPodChange(t *testing.T) {
	tests := []struct {
		name         string
		targetPod    *v1.Pod
		oldObj       interface{}
		newObj       interface{}
		expectedHint framework.QueueingHint
		expectErr    bool
	}{
		{
			name:         "same namespace protected gpu pod deleted",
			targetPod:    makeGPUPod("incoming", "team-a", "protected", "", 1),
			oldObj:       makeGPUPod("running", "team-a", "protected", "node-a", 2),
			expectedHint: framework.Queue,
		},
		{
			name:         "other namespace protected gpu pod deleted",
			targetPod:    makeGPUPod("incoming", "team-a", "protected", "", 1),
			oldObj:       makeGPUPod("running", "team-b", "protected", "node-a", 2),
			expectedHint: framework.QueueSkip,
		},
		{
			name:         "same namespace normal pod deleted",
			targetPod:    makeGPUPod("incoming", "team-a", "protected", "", 1),
			oldObj:       makeGPUPod("running", "team-a", "normal", "node-a", 2),
			expectedHint: framework.QueueSkip,
		},
		{
			name:         "target pod scaled down",
			targetPod:    makeGPUPod("incoming", "team-a", "protected", "", 2),
			oldObj:       makeGPUPodWithUID("incoming", "team-a", "protected", "", 2, "incoming-uid"),
			newObj:       makeGPUPodWithUID("incoming", "team-a", "protected", "", 1, "incoming-uid"),
			expectedHint: framework.Queue,
		},
		{
			name:         "wrong object type returns error and queues",
			targetPod:    makeGPUPod("incoming", "team-a", "protected", "", 1),
			oldObj:       "not-a-pod",
			expectedHint: framework.Queue,
			expectErr:    true,
		},
	}

	pl := &NamespaceResourceGuarantee{args: newArgs(map[string]int64{"team-a": 1})}
	logger, _ := ktesting.NewTestContext(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint, err := pl.isSchedulableAfterPodChange(logger, tt.targetPod, tt.oldObj, tt.newObj)
			if (err != nil) != tt.expectErr {
				t.Fatalf("unexpected error presence: got %v, want error=%v", err, tt.expectErr)
			}
			if hint != tt.expectedHint {
				t.Fatalf("unexpected hint: got %v, want %v", hint, tt.expectedHint)
			}
		})
	}
}

func newArgs(guarantees map[string]int64) config.NamespaceResourceGuaranteeArgs {
	return config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		GPUResourceName:            "nvidia.com/gpu",
		NamespaceGuarantees:        guarantees,
	}
}

func makeGPUNode(name string, gpu int64) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(gpu, resource.DecimalSI),
			},
		},
	}
}

func makeGPUPod(name, namespace, priorityClassName, nodeName string, gpu int64) *v1.Pod {
	return makeGPUPodWithUID(name, namespace, priorityClassName, nodeName, gpu, name+"-uid")
}

func makeGPUPodWithUID(name, namespace, priorityClassName, nodeName string, gpu int64, uid string) *v1.Pod {
	resources := v1.ResourceRequirements{}
	if gpu > 0 {
		resources.Requests = v1.ResourceList{
			v1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(gpu, resource.DecimalSI),
		}
		resources.Limits = v1.ResourceList{
			v1.ResourceName("nvidia.com/gpu"): *resource.NewQuantity(gpu, resource.DecimalSI),
		}
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       typesUID(uid),
		},
		Spec: v1.PodSpec{
			NodeName:          nodeName,
			PriorityClassName: priorityClassName,
			Containers: []v1.Container{
				{
					Name:      "c",
					Image:     "pause",
					Resources: resources,
				},
			},
		},
	}
}

func makeCPUPod(name, namespace, priorityClassName string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1.PodSpec{
			PriorityClassName: priorityClassName,
			Containers: []v1.Container{
				{
					Name:  "c",
					Image: "pause",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: *resource.NewMilliQuantity(500, resource.DecimalSI),
						},
					},
				},
			},
		},
	}
}

func typesUID(value string) types.UID {
	return types.UID(value)
}
