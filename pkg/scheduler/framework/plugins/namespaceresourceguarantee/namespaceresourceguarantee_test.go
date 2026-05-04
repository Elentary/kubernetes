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
			name: "protected pod within cpu and memory guarantees passes",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceCPU:    resource.MustParse("4"),
					v1.ResourceMemory: resource.MustParse("16Gi"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU:    "1",
				v1.ResourceMemory: "1Gi",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("running", "team-a", "protected", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU:    "2",
					v1.ResourceMemory: "4Gi",
				}),
			},
			nodes:    []*v1.Node{makeNode("node-a")},
			wantCode: framework.Success,
		},
		{
			name: "protected pod exceeding cpu guarantee fails",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceCPU: resource.MustParse("4"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1500m",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("running", "team-a", "protected", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU: "3",
				}),
			},
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" protected resource guarantee exceeded: resource="cpu" guarantee=4000 current=3000 requested=1500`,
		},
		{
			name: "missing namespace guarantee defaults to zero",
			args: newArgs(map[string]v1.ResourceList{
				"team-b": {
					v1.ResourceCPU: resource.MustParse("1"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "100m",
			}),
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" protected resource guarantee exceeded: resource="cpu" guarantee=0 current=0 requested=100`,
		},
		{
			name: "normal pod bypasses plugin",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceCPU: resource.MustParse("0"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "normal", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "8",
			}),
			nodes:    []*v1.Node{makeNode("node-a")},
			wantCode: framework.Success,
		},
		{
			name: "protected pod requesting non-configured resource bypasses cap accounting",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceCPU: resource.MustParse("1"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceName("nvidia.com/gpu"): "8",
			}),
			nodes:    []*v1.Node{makeNode("node-a")},
			wantCode: framework.Success,
		},
		{
			name: "only same namespace protected pods are counted",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceCPU: resource.MustParse("2"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("other-ns", "team-b", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "8"}),
				makePodWithRequests("normal", "team-a", "normal", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "8"}),
			},
			nodes:    []*v1.Node{makeNode("node-a")},
			wantCode: framework.Success,
		},
		{
			name: "protected pod exceeding gpu guarantee fails",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {
					v1.ResourceName("nvidia.com/gpu"): resource.MustParse("3"),
				},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceName("nvidia.com/gpu"): "2",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("running", "team-a", "protected", "node-a", map[v1.ResourceName]string{
					v1.ResourceName("nvidia.com/gpu"): "2",
				}),
			},
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" protected resource guarantee exceeded: resource="nvidia.com/gpu" guarantee=3 current=2 requested=2`,
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
			name: "same namespace protected cpu pod deleted",
			targetPod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			oldObj: makePodWithRequests("running", "team-a", "protected", "node-a", map[v1.ResourceName]string{
				v1.ResourceCPU: "2",
			}),
			expectedHint: framework.Queue,
		},
		{
			name: "other namespace protected cpu pod deleted",
			targetPod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			oldObj: makePodWithRequests("running", "team-b", "protected", "node-a", map[v1.ResourceName]string{
				v1.ResourceCPU: "2",
			}),
			expectedHint: framework.QueueSkip,
		},
		{
			name: "same namespace normal pod deleted",
			targetPod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			oldObj: makePodWithRequests("running", "team-a", "normal", "node-a", map[v1.ResourceName]string{
				v1.ResourceCPU: "2",
			}),
			expectedHint: framework.QueueSkip,
		},
		{
			name: "target pod scaled down memory",
			targetPod: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceMemory: "2Gi",
			}, "incoming-uid"),
			oldObj: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceMemory: "2Gi",
			}, "incoming-uid"),
			newObj: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceMemory: "1Gi",
			}, "incoming-uid"),
			expectedHint: framework.Queue,
		},
		{
			name: "target pod update with same requests",
			targetPod: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}, "incoming-uid"),
			oldObj: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}, "incoming-uid"),
			newObj: makePodWithRequestsAndUID("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}, "incoming-uid"),
			expectedHint: framework.QueueSkip,
		},
		{
			name:         "wrong object type returns error and queues",
			targetPod:    makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
			oldObj:       "not-a-pod",
			expectedHint: framework.Queue,
			expectErr:    true,
		},
	}

	pl := &NamespaceResourceGuarantee{
		args: newArgs(map[string]v1.ResourceList{
			"team-a": {
				v1.ResourceCPU:    resource.MustParse("1"),
				v1.ResourceMemory: resource.MustParse("2Gi"),
			},
		}),
		configuredResource: []v1.ResourceName{v1.ResourceCPU, v1.ResourceMemory},
	}
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

func newArgs(guarantees map[string]v1.ResourceList) config.NamespaceResourceGuaranteeArgs {
	return config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		NamespaceGuarantees:        guarantees,
	}
}

func makeNode(name string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourceCPU:                    resource.MustParse("128"),
				v1.ResourceMemory:                 resource.MustParse("1Ti"),
				v1.ResourceName("nvidia.com/gpu"): resource.MustParse("16"),
			},
		},
	}
}

func makePodWithRequests(name, namespace, priorityClassName, nodeName string, requests map[v1.ResourceName]string) *v1.Pod {
	return makePodWithRequestsAndUID(name, namespace, priorityClassName, nodeName, requests, name+"-uid")
}

func makePodWithRequestsAndUID(name, namespace, priorityClassName, nodeName string, requests map[v1.ResourceName]string, uid string) *v1.Pod {
	resourceRequests := make(v1.ResourceList, len(requests))
	resourceLimits := make(v1.ResourceList, len(requests))
	for resourceName, quantity := range requests {
		parsed := resource.MustParse(quantity)
		resourceRequests[resourceName] = parsed
		resourceLimits[resourceName] = parsed
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
					Name:  "c",
					Image: "pause",
					Resources: v1.ResourceRequirements{
						Requests: resourceRequests,
						Limits:   resourceLimits,
					},
				},
			},
		},
	}
}

func typesUID(value string) types.UID {
	return types.UID(value)
}
