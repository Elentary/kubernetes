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
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clientgoevents "k8s.io/client-go/tools/events"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	ktesting "k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
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

func TestCompareNamespaceForEviction(t *testing.T) {
	deficits := []resourceDeficit{
		{resourceName: v1.ResourceCPU, deficit: 4000, request: 8000},
		{resourceName: v1.ResourceMemory, deficit: 8 << 30, request: 16 << 30},
	}

	t.Run("higher usage on most deficient resource is less important", func(t *testing.T) {
		usage := map[string]map[v1.ResourceName]int64{
			"team-a": {v1.ResourceCPU: 2000, v1.ResourceMemory: 12 << 30},
			"team-b": {v1.ResourceCPU: 4000, v1.ResourceMemory: 1 << 30},
		}
		got, ok := compareNamespaceForEviction("team-a", "team-b", "team-c", deficits, usage)
		if !ok {
			t.Fatalf("expected namespace comparison to apply")
		}
		if got != -1 {
			t.Fatalf("unexpected compare result: got %d, want -1", got)
		}
	})

	t.Run("equal usage prefers evicting preemptor namespace", func(t *testing.T) {
		usage := map[string]map[v1.ResourceName]int64{
			"team-a": {v1.ResourceCPU: 2000, v1.ResourceMemory: 4 << 30},
			"team-b": {v1.ResourceCPU: 2000, v1.ResourceMemory: 4 << 30},
		}
		got, ok := compareNamespaceForEviction("team-a", "team-b", "team-a", deficits, usage)
		if !ok {
			t.Fatalf("expected namespace comparison to apply")
		}
		if got != 1 {
			t.Fatalf("unexpected compare result: got %d, want 1", got)
		}
	})

	t.Run("unlisted namespace uses default tie-break", func(t *testing.T) {
		usage := map[string]map[v1.ResourceName]int64{
			"team-a": {v1.ResourceCPU: 1},
		}
		_, ok := compareNamespaceForEviction("team-a", "team-x", "team-a", deficits, usage)
		if ok {
			t.Fatalf("expected namespace comparison to be skipped for unlisted namespace")
		}
	})
}

func TestOrderedDeficientResources(t *testing.T) {
	node := makeNode("node-a")
	node.Status.Allocatable = v1.ResourceList{
		v1.ResourceCPU:                    resource.MustParse("10"),
		v1.ResourceMemory:                 resource.MustParse("20Gi"),
		v1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
	}
	existing := []*v1.Pod{
		makePodWithRequests("running", "team-a", "normal", "node-a", map[v1.ResourceName]string{
			v1.ResourceCPU:                    "4",
			v1.ResourceMemory:                 "10Gi",
			v1.ResourceName("nvidia.com/gpu"): "3",
		}),
	}
	nodeInfo := framework.NewNodeInfo(existing...)
	nodeInfo.SetNode(node)

	incoming := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
		v1.ResourceCPU:                    "8",
		v1.ResourceMemory:                 "16Gi",
		v1.ResourceName("nvidia.com/gpu"): "2",
	})

	pl := &NamespaceResourceGuarantee{
		configuredResource: []v1.ResourceName{
			v1.ResourceCPU,
			v1.ResourceMemory,
			v1.ResourceName("nvidia.com/gpu"),
		},
	}
	got := pl.orderedDeficientResources(nodeInfo, incoming)
	gotNames := make([]v1.ResourceName, 0, len(got))
	for _, deficit := range got {
		gotNames = append(gotNames, deficit.resourceName)
	}
	want := []v1.ResourceName{
		v1.ResourceName("nvidia.com/gpu"),
		v1.ResourceMemory,
		v1.ResourceCPU,
	}
	if diff := cmp.Diff(want, gotNames); diff != "" {
		t.Fatalf("unexpected deficient resource order (-want,+got):\n%s", diff)
	}
}

func TestClassifyPreemptionEvent(t *testing.T) {
	traceWithVictims := &preemptionDecisionTrace{
		nodes: map[string]*nodePreemptionTrace{
			"node-a": {
				nodeName: "node-a",
				victims: []*v1.Pod{
					makePodWithRequests("victim-a", "team-b", "normal", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
					makePodWithRequests("victim-b", "team-c", "normal", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
				},
			},
		},
	}

	tests := []struct {
		name                string
		pod                 *v1.Pod
		result              *framework.PostFilterResult
		status              *framework.Status
		trace               *preemptionDecisionTrace
		wantEventType       string
		wantReason          string
		wantNoteContains    []string
		wantNoteNotContains []string
	}{
		{
			name:          "started event says initiated not completed",
			pod:           makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
			result:        framework.NewPostFilterResultWithNominatedNode("node-a"),
			status:        framework.NewStatus(framework.Success),
			trace:         traceWithVictims,
			wantEventType: v1.EventTypeNormal,
			wantReason:    preemptionStartedReason,
			wantNoteContains: []string{
				"phase=started",
				"nominatedNode=node-a",
				"victims=[team-b/victim-a,team-c/victim-b]",
				"preemption initiated; victim termination may still be in progress",
			},
		},
		{
			name: "waiting event uses existing nominated node and omits empty victims",
			pod: func() *v1.Pod {
				p := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
				p.Status.NominatedNodeName = "node-b"
				return p
			}(),
			status:        framework.NewStatus(framework.Unschedulable, preemptionWaitingOnTerminatingVictims),
			trace:         &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{}},
			wantEventType: v1.EventTypeNormal,
			wantReason:    preemptionWaitingReason,
			wantNoteContains: []string{
				"phase=waiting",
				"nominatedNode=node-b",
				`reason="waiting for preempted pods to terminate"`,
			},
			wantNoteNotContains: []string{"victims=[]"},
		},
		{
			name:          "not helpful event is explicit",
			pod:           makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
			status:        framework.NewStatus(framework.Unschedulable, "0/10 nodes are available: 10 Preemption is not helpful for scheduling."),
			trace:         &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{}},
			wantEventType: v1.EventTypeNormal,
			wantReason:    preemptionNotHelpfulReason,
			wantNoteContains: []string{
				"phase=not-helpful",
				preemptionNotHelpfulFragment,
			},
		},
		{
			name:          "no candidate event is explicit",
			pod:           makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
			status:        framework.NewStatus(framework.Unschedulable, preemptionNoCandidateMessage),
			trace:         &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{}},
			wantEventType: v1.EventTypeNormal,
			wantReason:    preemptionNoCandidateReason,
			wantNoteContains: []string{
				"phase=no-candidate",
				preemptionNoCandidateMessage,
			},
		},
		{
			name:          "error event is warning",
			pod:           makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
			status:        framework.NewStatus(framework.Error, "boom"),
			trace:         &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{}},
			wantEventType: v1.EventTypeWarning,
			wantReason:    preemptionErrorReason,
			wantNoteContains: []string{
				"phase=error",
				`reason="boom"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyPreemptionEvent(tt.pod, tt.result, tt.status, tt.trace)
			if got.eventType != tt.wantEventType {
				t.Fatalf("unexpected event type: got %q, want %q", got.eventType, tt.wantEventType)
			}
			if got.reason != tt.wantReason {
				t.Fatalf("unexpected reason: got %q, want %q", got.reason, tt.wantReason)
			}
			for _, want := range tt.wantNoteContains {
				if !strings.Contains(got.note, want) {
					t.Fatalf("expected note %q to contain %q", got.note, want)
				}
			}
			for _, unwanted := range tt.wantNoteNotContains {
				if strings.Contains(got.note, unwanted) {
					t.Fatalf("expected note %q to omit %q", got.note, unwanted)
				}
			}
		})
	}
}

func TestLogPreemptionDecisionEmitsEvent(t *testing.T) {
	recorder := clientgoevents.NewFakeRecorder(4)
	fh, err := frameworkruntime.NewFramework(context.Background(), nil, nil, frameworkruntime.WithEventRecorder(recorder))
	if err != nil {
		t.Fatalf("Failed creating framework runtime: %v", err)
	}

	plugin := &NamespaceResourceGuarantee{handle: fh}
	pod := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	pod.Status.NominatedNodeName = "node-a"
	trace := &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{}}
	plugin.preemptionTrace.Store(pod.UID, trace)
	defer plugin.preemptionTrace.Delete(pod.UID)

	plugin.logPreemptionDecision(context.Background(), pod, nil, framework.NewStatus(framework.Unschedulable, preemptionWaitingOnTerminatingVictims))

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, preemptionWaitingReason) {
			t.Fatalf("expected waiting reason in event, got %q", event)
		}
		if !strings.Contains(event, "phase=waiting") {
			t.Fatalf("expected waiting phase in event, got %q", event)
		}
		if !strings.Contains(event, "nominatedNode=node-a") {
			t.Fatalf("expected nominated node in event, got %q", event)
		}
		if strings.Contains(event, "victims=[]") {
			t.Fatalf("expected no empty victims list in event, got %q", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestPreFilterDoesNotEmitEvents(t *testing.T) {
	ctx := context.Background()
	recorder := clientgoevents.NewFakeRecorder(1)
	fh, err := frameworkruntime.NewFramework(ctx, nil, nil, frameworkruntime.WithEventRecorder(recorder), frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot(nil, []*v1.Node{makeNode("node-a")})))
	if err != nil {
		t.Fatalf("Failed creating framework runtime: %v", err)
	}

	plugin := &NamespaceResourceGuarantee{
		handle:             fh,
		args:               newArgs(map[string]v1.ResourceList{"team-a": {v1.ResourceCPU: resource.MustParse("4")}}),
		configuredResource: []v1.ResourceName{v1.ResourceCPU},
	}
	pod := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})

	_, status := plugin.PreFilter(ctx, framework.NewCycleState(), pod)
	if status != nil && !status.IsSuccess() {
		t.Fatalf("unexpected status: %v", status)
	}

	select {
	case event := <-recorder.Events:
		t.Fatalf("did not expect any prefilter event, got %q", event)
	default:
	}
}

var nodeResourcesFitFunc = frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewFit)

func TestPostFilterEmitsStartedEventEndToEnd(t *testing.T) {
	metrics.Register()

	preemptor := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
		v1.ResourceCPU: "1200m",
	})
	preemptor.Spec.Priority = ptrTo(int32(1000))
	victim := makePodWithRequests("victim", "team-b", "normal", "node-a", map[v1.ResourceName]string{
		v1.ResourceCPU: "1000m",
	})
	victim.Spec.Priority = ptrTo(int32(0))

	node := makeNode("node-a")
	node.Status.Allocatable = v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("1500m"),
		v1.ResourceMemory: resource.MustParse("1Ti"),
		v1.ResourcePods:   resource.MustParse("32"),
	}

	cs := clientsetfake.NewClientset(&v1.PodList{Items: []v1.Pod{*preemptor, *victim}})
	informerFactory := informers.NewSharedInformerFactory(cs, 0)
	podInformer := informerFactory.Core().V1().Pods().Informer()
	if err := podInformer.GetStore().Add(preemptor); err != nil {
		t.Fatal(err)
	}
	if err := podInformer.GetStore().Add(victim); err != nil {
		t.Fatal(err)
	}

	recorder := clientgoevents.NewFakeRecorder(4)
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fh, err := tf.NewFramework(ctx,
		[]tf.RegisterPluginFunc{
			tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
			tf.RegisterPluginAsExtensions(noderesources.Name, nodeResourcesFitFunc, "Filter", "PreFilter"),
			tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		},
		"",
		frameworkruntime.WithClientSet(cs),
		frameworkruntime.WithEventRecorder(recorder),
		frameworkruntime.WithInformerFactory(informerFactory),
		frameworkruntime.WithPodNominator(noopPodNominator{}),
		frameworkruntime.WithSnapshotSharedLister(internalcache.NewSnapshot([]*v1.Pod{victim}, []*v1.Node{node})),
		frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
		frameworkruntime.WithLogger(logger),
	)
	if err != nil {
		t.Fatal(err)
	}

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	pluginIface, err := New(ctx, runtime.Object(&config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		NamespaceGuarantees: map[string]v1.ResourceList{
			"team-a": {v1.ResourceCPU: resource.MustParse("10")},
		},
	}), fh)
	if err != nil {
		t.Fatal(err)
	}
	plugin := pluginIface.(*NamespaceResourceGuarantee)

	state := framework.NewCycleState()
	if _, status, _ := fh.RunPreFilterPlugins(ctx, state, preemptor); !status.IsSuccess() {
		t.Fatalf("unexpected prefilter status: %v", status)
	}

	nodeToStatus := framework.NewDefaultNodeToStatus()
	nodeToStatus.Set("node-a", framework.NewStatus(framework.Unschedulable))

	result, status := plugin.PostFilter(ctx, state, preemptor, nodeToStatus)
	if !status.IsSuccess() {
		t.Fatalf("unexpected postfilter status: %v", status)
	}
	if result == nil || result.NominatedNodeName != "node-a" {
		t.Fatalf("unexpected postfilter result: %#v", result)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-recorder.Events:
			if !strings.Contains(event, preemptionStartedReason) {
				continue
			}
			if !strings.Contains(event, "phase=started") {
				t.Fatalf("expected started phase in event, got %q", event)
			}
			if !strings.Contains(event, "nominatedNode=node-a") {
				t.Fatalf("expected nominated node in event, got %q", event)
			}
			if !strings.Contains(event, "team-b/victim") {
				t.Fatalf("expected victim key in event, got %q", event)
			}
			if !strings.Contains(event, "victim termination may still be in progress") {
				t.Fatalf("expected non-terminal note in event, got %q", event)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for plugin started event")
		}
	}
}

func TestNewRecordsConfiguredQuotaMetrics(t *testing.T) {
	namespaceResourceGuaranteeQuota.Reset()
	namespaceResourceGuaranteeProtectedPriorityClassInfo.Reset()
	t.Cleanup(func() {
		namespaceResourceGuaranteeQuota.Reset()
		namespaceResourceGuaranteeProtectedPriorityClassInfo.Reset()
	})

	ctx := context.Background()
	fh, err := frameworkruntime.NewFramework(ctx, nil, &config.KubeSchedulerProfile{SchedulerName: "test-profile"})
	if err != nil {
		t.Fatalf("Failed creating framework runtime: %v", err)
	}

	args := &config.NamespaceResourceGuaranteeArgs{
		ProtectedPriorityClassName: "protected",
		NamespaceGuarantees: map[string]v1.ResourceList{
			"team-a": {
				v1.ResourceCPU:                    resource.MustParse("4"),
				v1.ResourceMemory:                 resource.MustParse("16Gi"),
				v1.ResourceName("nvidia.com/gpu"): resource.MustParse("3"),
			},
		},
	}
	if _, err := New(ctx, args, fh); err != nil {
		t.Fatalf("unexpected error creating plugin: %v", err)
	}

	expected := `
		# HELP scheduler_namespace_resource_guarantee_protected_priority_class_info [ALPHA] Information about the configured protected priority class for NamespaceResourceGuarantee plugin.
		# TYPE scheduler_namespace_resource_guarantee_protected_priority_class_info gauge
		scheduler_namespace_resource_guarantee_protected_priority_class_info{priority_class="protected",profile="test-profile"} 1
		# HELP scheduler_namespace_resource_guarantee_quota [ALPHA] Configured per-namespace protected resource quota for NamespaceResourceGuarantee plugin.
		# TYPE scheduler_namespace_resource_guarantee_quota gauge
		scheduler_namespace_resource_guarantee_quota{namespace="team-a",profile="test-profile",resource="cpu",unit="millicore"} 4000
		scheduler_namespace_resource_guarantee_quota{namespace="team-a",profile="test-profile",resource="memory",unit="byte"} 1.7179869184e+10
		scheduler_namespace_resource_guarantee_quota{namespace="team-a",profile="test-profile",resource="nvidia.com/gpu",unit="unit"} 3
	`
	if err := testutil.GatherAndCompare(
		metrics.GetGather(),
		strings.NewReader(expected),
		"scheduler_namespace_resource_guarantee_protected_priority_class_info",
		"scheduler_namespace_resource_guarantee_quota",
	); err != nil {
		t.Fatal(err)
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

func ptrTo[T any](v T) *T {
	return &v
}

type noopPodNominator struct{}

func (noopPodNominator) AddNominatedPod(klog.Logger, *framework.PodInfo, *framework.NominatingInfo) {}

func (noopPodNominator) DeleteNominatedPodIfExists(*v1.Pod) {}

func (noopPodNominator) UpdateNominatedPod(klog.Logger, *v1.Pod, *framework.PodInfo) {}

func (noopPodNominator) NominatedPodsForNode(string) []*framework.PodInfo { return nil }
