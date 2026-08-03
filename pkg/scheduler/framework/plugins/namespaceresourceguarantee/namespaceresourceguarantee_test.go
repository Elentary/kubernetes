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
	"errors"
	"fmt"
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
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nominatednodereservation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	plugintesting "k8s.io/kubernetes/pkg/scheduler/framework/plugins/testing"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	testktesting "k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
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
			wantMessage: `namespace "team-a" shared protected resource guarantee exceeded: resource="cpu" guarantee=4000 current=3000 requested=1500`,
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
			wantMessage: `namespace "team-a" shared protected resource guarantee exceeded: resource="cpu" guarantee=0 current=0 requested=100`,
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
			wantMessage: `namespace "team-a" shared protected resource guarantee exceeded: resource="nvidia.com/gpu" guarantee=3 current=2 requested=2`,
		},
		{
			name: "semi-guaranteed pod is capped by combined guaranteed usage",
			args: tieredArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceCPU: resource.MustParse("4")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "semi", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "3",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("guaranteed", "team-a", "protected", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU: "4",
				}),
				makePodWithRequests("semi", "team-a", "semi", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU: "1",
				}),
			},
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" shared protected resource guarantee exceeded: resource="cpu" guarantee=4000 current=5000 requested=3000`,
		},
		{
			name: "guaranteed pod is capped by combined semi-guaranteed usage",
			args: tieredArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceCPU: resource.MustParse("4")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "3",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("semi", "team-a", "semi", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU: "4",
				}),
				makePodWithRequests("guaranteed", "team-a", "protected", "node-a", map[v1.ResourceName]string{
					v1.ResourceCPU: "1",
				}),
			},
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-a" shared protected resource guarantee exceeded: resource="cpu" guarantee=4000 current=5000 requested=3000`,
		},
		{
			name: "semi-guaranteed pod in an unmanaged namespace is capped at zero",
			args: tieredArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceCPU: resource.MustParse("4")},
			}),
			pod: makePodWithRequests("incoming", "team-b", "semi", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			nodes:       []*v1.Node{makeNode("node-a")},
			wantCode:    framework.UnschedulableAndUnresolvable,
			wantMessage: `namespace "team-b" shared protected resource guarantee exceeded: resource="cpu" guarantee=0 current=0 requested=1000`,
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

func TestSelectVictimsOnNodeRespectsManagedNamespaceRestriction(t *testing.T) {
	metrics.Register()

	tests := []struct {
		name             string
		restrict         bool
		preemptorClass   string
		victimNamespaces []string
		allocatableCPU   string
		wantVictimNames  []string
		wantStatusCode   framework.Code
	}{
		{
			name:             "guaranteed preemptor selects only managed namespace victims when restricted",
			restrict:         true,
			preemptorClass:   "protected",
			victimNamespaces: []string{"team-b", "unmanaged"},
			allocatableCPU:   "2",
			wantVictimNames:  []string{"victim-0"},
			wantStatusCode:   framework.Success,
		},
		{
			name:             "guaranteed preemptor cannot select only unmanaged victims when restricted",
			restrict:         true,
			preemptorClass:   "protected",
			victimNamespaces: []string{"unmanaged"},
			allocatableCPU:   "1",
			wantStatusCode:   framework.UnschedulableAndUnresolvable,
		},
		{
			name:             "omitted restriction retains unmanaged victim eligibility",
			preemptorClass:   "protected",
			victimNamespaces: []string{"unmanaged"},
			allocatableCPU:   "1",
			wantVictimNames:  []string{"victim-0"},
			wantStatusCode:   framework.Success,
		},
		{
			name:             "restriction does not apply to semi-guaranteed preemptor",
			restrict:         true,
			preemptorClass:   "semi",
			victimNamespaces: []string{"unmanaged"},
			allocatableCPU:   "1",
			wantVictimNames:  []string{"victim-0"},
			wantStatusCode:   framework.Success,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, ctx := ktesting.NewTestContext(t)
			preemptor := makePodWithRequests("incoming", "team-a", tt.preemptorClass, "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
			preemptor.Spec.Priority = ptrTo(int32(1000))
			victims := make([]*v1.Pod, 0, len(tt.victimNamespaces))
			for i, namespace := range tt.victimNamespaces {
				victim := makePodWithRequests(fmt.Sprintf("victim-%d", i), namespace, "normal", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
				victim.Spec.Priority = ptrTo(int32(0))
				victims = append(victims, victim)
			}
			node := makeNode("node-a")
			node.Status.Allocatable[v1.ResourceCPU] = resource.MustParse(tt.allocatableCPU)
			node.Status.Allocatable[v1.ResourcePods] = resource.MustParse("100")
			snapshot := internalcache.NewSnapshot(victims, []*v1.Node{node})

			fh, err := tf.NewFramework(ctx,
				[]tf.RegisterPluginFunc{
					tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
					tf.RegisterPluginAsExtensions(noderesources.Name, nodeResourcesFitFunc, "Filter", "PreFilter"),
					tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				},
				"",
				frameworkruntime.WithSnapshotSharedLister(snapshot),
				frameworkruntime.WithLogger(logger),
				frameworkruntime.WithPodNominator(noopPodNominator{}),
				frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
			)
			if err != nil {
				t.Fatalf("creating framework: %v", err)
			}
			args := tieredArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceCPU: resource.MustParse("10")},
				"team-b": {v1.ResourceCPU: resource.MustParse("10")},
			})
			args.RestrictGuaranteedPreemptionToManagedNamespaces = tt.restrict
			plugin := &NamespaceResourceGuarantee{
				handle:             fh,
				args:               args,
				configuredResource: []v1.ResourceName{v1.ResourceCPU},
			}
			state := framework.NewCycleState()
			if _, status, _ := fh.RunPreFilterPlugins(ctx, state, preemptor); !status.IsSuccess() {
				t.Fatalf("running prefilter plugins: %v", status)
			}
			nodeInfo, err := snapshot.NodeInfos().Get(node.Name)
			if err != nil {
				t.Fatalf("getting node info: %v", err)
			}
			got, _, status := plugin.SelectVictimsOnNode(ctx, state, preemptor, nodeInfo, nil)
			if status.Code() != tt.wantStatusCode {
				t.Fatalf("unexpected status: got %v (%s), want %v", status.Code(), status.Message(), tt.wantStatusCode)
			}
			var gotNames []string
			for _, victim := range got {
				gotNames = append(gotNames, victim.Name)
			}
			if diff := cmp.Diff(tt.wantVictimNames, gotNames); diff != "" {
				t.Fatalf("unexpected victims (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreFilterMetrics(t *testing.T) {
	resetMetricsForTest()
	t.Cleanup(resetMetricsForTest)

	ctx := context.Background()
	args := tieredArgs(map[string]v1.ResourceList{
		"team-a": {v1.ResourceCPU: resource.MustParse("4")},
	})
	newPlugin := func(t *testing.T, lister framework.SharedLister) *NamespaceResourceGuarantee {
		t.Helper()
		fh, err := frameworkruntime.NewFramework(
			ctx,
			nil,
			&config.KubeSchedulerProfile{SchedulerName: "test-profile"},
			frameworkruntime.WithSnapshotSharedLister(lister),
		)
		if err != nil {
			t.Fatalf("failed creating framework runtime: %v", err)
		}
		plugin, err := New(ctx, &args, fh)
		if err != nil {
			t.Fatalf("failed creating plugin: %v", err)
		}
		return plugin.(*NamespaceResourceGuarantee)
	}

	allowed := makePodWithRequests("allowed", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	allowedPlugin := newPlugin(t, internalcache.NewSnapshot(nil, []*v1.Node{makeNode("node-a")}))
	if _, status := allowedPlugin.PreFilter(ctx, framework.NewCycleState(), allowed); status != nil {
		t.Fatalf("expected allowed pod to pass, got %v", status)
	}

	existing := makePodWithRequests("running", "team-a", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceCPU: "3"})
	exceeded := makePodWithRequests("exceeded", "team-a", "semi", "", map[v1.ResourceName]string{v1.ResourceCPU: "2"})
	exceededPlugin := newPlugin(t, internalcache.NewSnapshot([]*v1.Pod{existing}, []*v1.Node{makeNode("node-a")}))
	if _, status := exceededPlugin.PreFilter(ctx, framework.NewCycleState(), exceeded); status == nil || status.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("expected quota-exceeded status, got %v", status)
	}

	failed := makePodWithRequests("failed", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	failedPlugin := newPlugin(t, errorSharedLister{})
	if _, status := failedPlugin.PreFilter(ctx, framework.NewCycleState(), failed); status == nil || status.Code() != framework.Error {
		t.Fatalf("expected error status, got %v", status)
	}

	expected := `
		# HELP scheduler_namespace_resource_guarantee_prefilter_decisions_total [ALPHA] Number of protected-pod namespace resource guarantee PreFilter decisions.
		# TYPE scheduler_namespace_resource_guarantee_prefilter_decisions_total counter
		scheduler_namespace_resource_guarantee_prefilter_decisions_total{namespace="team-a",profile="test-profile",result="allowed",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_prefilter_decisions_total{namespace="team-a",profile="test-profile",result="error",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_prefilter_decisions_total{namespace="team-a",profile="test-profile",result="quota_exceeded",tier="semi-guaranteed"} 1
		# HELP scheduler_namespace_resource_guarantee_quota_exceeded_total [ALPHA] Number of protected-pod PreFilter decisions rejected because a namespace resource guarantee would be exceeded.
		# TYPE scheduler_namespace_resource_guarantee_quota_exceeded_total counter
		scheduler_namespace_resource_guarantee_quota_exceeded_total{namespace="team-a",profile="test-profile",resource="cpu",tier="semi-guaranteed"} 1
	`
	if err := testutil.GatherAndCompare(
		metrics.GetGather(),
		strings.NewReader(expected),
		"scheduler_namespace_resource_guarantee_prefilter_decisions_total",
		"scheduler_namespace_resource_guarantee_quota_exceeded_total",
	); err != nil {
		t.Fatal(err)
	}
}

func TestPreemptionOutcomeMetrics(t *testing.T) {
	resetMetricsForTest()
	t.Cleanup(resetMetricsForTest)

	plugin := &NamespaceResourceGuarantee{
		profile: "test-profile",
		args:    tieredArgs(nil),
	}
	protected := makePodWithRequests("protected", "team-a", "protected", "", nil)
	ordinary := makePodWithRequests("ordinary", "team-a", "normal", "", nil)

	for _, reason := range []string{
		preemptionStartedReason,
		preemptionWaitingReason,
		preemptionNotHelpfulReason,
		preemptionNoCandidateReason,
		preemptionErrorReason,
	} {
		plugin.recordPreemptionOutcome(protected, string(plugin.podTier(protected)), preemptionOutcomeForEventReason(reason))
	}
	plugin.recordPreemptionOutcome(ordinary, "ordinary", preemptionOutcomeIneligible)

	expected := `
		# HELP scheduler_namespace_resource_guarantee_preemption_outcomes_total [ALPHA] Number of NamespaceResourceGuarantee PostFilter outcomes.
		# TYPE scheduler_namespace_resource_guarantee_preemption_outcomes_total counter
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="error",profile="test-profile",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="ineligible",profile="test-profile",tier="ordinary"} 1
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="no_candidate",profile="test-profile",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="not_helpful",profile="test-profile",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="started",profile="test-profile",tier="guaranteed"} 1
		scheduler_namespace_resource_guarantee_preemption_outcomes_total{namespace="team-a",outcome="waiting",profile="test-profile",tier="guaranteed"} 1
	`
	if err := testutil.GatherAndCompare(
		metrics.GetGather(),
		strings.NewReader(expected),
		"scheduler_namespace_resource_guarantee_preemption_outcomes_total",
	); err != nil {
		t.Fatal(err)
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

func TestScore(t *testing.T) {
	makeGPUNode := func(name string, gpu string) *v1.Node {
		node := makeNode(name)
		node.Status.Allocatable[v1.ResourceName("nvidia.com/gpu")] = resource.MustParse(gpu)
		return node
	}

	tests := []struct {
		name         string
		args         config.NamespaceResourceGuaranteeArgs
		pod          *v1.Pod
		existingPods []*v1.Pod
		nodes        []*v1.Node
		wantScores   map[string]int64
	}{
		{
			name: "protected gpu pod prefers node with existing protected gpu usage",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceName("nvidia.com/gpu"): "1",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("packed", "team-a", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "3"}),
			},
			nodes: []*v1.Node{makeGPUNode("node-a", "8"), makeGPUNode("node-b", "8")},
			wantScores: map[string]int64{
				"node-a": 50,
				"node-b": 12,
			},
		},
		{
			name: "protected pod without gpu request stays neutral",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("packed", "team-a", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "3"}),
			},
			nodes: []*v1.Node{makeGPUNode("node-a", "8"), makeGPUNode("node-b", "8")},
			wantScores: map[string]int64{
				"node-a": 0,
				"node-b": 0,
			},
		},
		{
			name: "normal gpu pod stays neutral",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "normal", "", map[v1.ResourceName]string{
				v1.ResourceName("nvidia.com/gpu"): "1",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("packed", "team-a", "protected", "node-a", map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "3"}),
			},
			nodes: []*v1.Node{makeGPUNode("node-a", "8"), makeGPUNode("node-b", "8")},
			wantScores: map[string]int64{
				"node-a": 0,
				"node-b": 0,
			},
		},
		{
			name: "normal gpu usage does not affect packing score",
			args: newArgs(map[string]v1.ResourceList{
				"team-a": {v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			}),
			pod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceName("nvidia.com/gpu"): "1",
			}),
			existingPods: []*v1.Pod{
				makePodWithRequests("normal-gpu", "team-a", "normal", "node-a", map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "4"}),
				makePodWithRequests("protected-gpu", "team-a", "protected", "node-b", map[v1.ResourceName]string{v1.ResourceName("nvidia.com/gpu"): "1"}),
			},
			nodes: []*v1.Node{makeGPUNode("node-a", "8"), makeGPUNode("node-b", "8")},
			wantScores: map[string]int64{
				"node-a": 12,
				"node-b": 25,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			plugin := plugintesting.SetupPlugin(ctx, t, New, &tt.args, internalcache.NewSnapshot(tt.existingPods, tt.nodes)).(*NamespaceResourceGuarantee)
			for nodeName, want := range tt.wantScores {
				got, status := plugin.Score(ctx, framework.NewCycleState(), tt.pod, nodeName)
				if status != nil && !status.IsSuccess() {
					t.Fatalf("unexpected score status for %s: %v", nodeName, status)
				}
				if got != want {
					t.Fatalf("unexpected score for %s: got %d, want %d", nodeName, got, want)
				}
			}
		})
	}
}

func TestPreemptionProtectedGPUPackingScore(t *testing.T) {
	makeGPUNode := func(name string, gpu string) *v1.Node {
		node := makeNode(name)
		node.Status.Allocatable[protectedGPUResource] = resource.MustParse(gpu)
		return node
	}

	incoming := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{protectedGPUResource: "1"})
	survivorA := makePodWithRequests("survivor-a", "team-a", "protected", "node-a", map[v1.ResourceName]string{protectedGPUResource: "3"})
	victimA := makePodWithRequests("victim-a", "team-b", "protected", "node-a", map[v1.ResourceName]string{protectedGPUResource: "2"})
	survivorB := makePodWithRequests("survivor-b", "team-a", "protected", "node-b", map[v1.ResourceName]string{protectedGPUResource: "1"})
	victimB := makePodWithRequests("victim-b", "team-b", "normal", "node-b", map[v1.ResourceName]string{protectedGPUResource: "2"})

	plugin := plugintesting.SetupPlugin(
		context.Background(),
		t,
		New,
		ptrTo(newArgs(map[string]v1.ResourceList{"team-a": {protectedGPUResource: resource.MustParse("8")}})),
		internalcache.NewSnapshot([]*v1.Pod{survivorA, victimA, survivorB, victimB}, []*v1.Node{makeGPUNode("node-a", "8"), makeGPUNode("node-b", "8")}),
	).(*NamespaceResourceGuarantee)

	scoreA, ok := plugin.preemptionProtectedGPUPackingScore(incoming, "node-a", &extenderv1.Victims{Pods: []*v1.Pod{victimA}})
	if !ok {
		t.Fatal("expected node-a preemption packing score")
	}
	if scoreA.protectedGPUBefore != 5 || scoreA.protectedGPUAfterVictims != 3 || scoreA.protectedGPUAfterScheduling != 4 || scoreA.score != 50 {
		t.Fatalf("unexpected node-a score details: %#v", scoreA)
	}

	scoreFuncs := plugin.OrderedScoreFuncs(context.Background(), incoming, map[string]*extenderv1.Victims{
		"node-a": {Pods: []*v1.Pod{victimA}},
		"node-b": {Pods: []*v1.Pod{victimB}},
	})
	if len(scoreFuncs) != 1 {
		t.Fatalf("expected one packing score func, got %d", len(scoreFuncs))
	}
	if scoreFuncs[0]("node-a") <= scoreFuncs[0]("node-b") {
		t.Fatalf("expected node-a to have higher packing score: node-a=%d node-b=%d", scoreFuncs[0]("node-a"), scoreFuncs[0]("node-b"))
	}

	nonGPUPod := makePodWithRequests("cpu-only", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	if got := plugin.OrderedScoreFuncs(context.Background(), nonGPUPod, map[string]*extenderv1.Victims{"node-a": {Pods: []*v1.Pod{victimA}}}); got != nil {
		t.Fatalf("expected no packing score funcs for non-GPU pod, got %d", len(got))
	}
}

func TestLogPreemptionDecisionEmitsCandidateScoreLogs(t *testing.T) {
	tCtx := testktesting.Init(t, initoption.BufferLogs(true))
	recorder := clientgoevents.NewFakeRecorder(4)
	fh, err := frameworkruntime.NewFramework(tCtx, nil, nil, frameworkruntime.WithEventRecorder(recorder))
	if err != nil {
		t.Fatalf("Failed creating framework runtime: %v", err)
	}

	plugin := &NamespaceResourceGuarantee{handle: fh}
	pod := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{protectedGPUResource: "1"})
	victim := makePodWithRequests("victim", "team-b", "normal", "node-a", map[v1.ResourceName]string{protectedGPUResource: "1"})
	state := framework.NewCycleState()
	framework.WriteSchedulingDecisionAttempt(state, 9)
	trace := &preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{
		"node-a": {
			nodeName:              "node-a",
			statusCode:            framework.Success,
			numPDBViolatingVictim: 1,
			victims:               []*v1.Pod{victim},
			packingScore: &preemptionCandidatePackingScore{
				score:                       50,
				incomingGPU:                 1,
				allocatableGPU:              8,
				protectedGPUBefore:          5,
				protectedGPUAfterVictims:    3,
				protectedGPUAfterScheduling: 4,
			},
		},
	}}
	plugin.preemptionTrace.Store(pod.UID, trace)
	defer plugin.preemptionTrace.Delete(pod.UID)

	plugin.logPreemptionDecision(tCtx, state, pod, framework.NewPostFilterResultWithNominatedNode("node-a"), framework.NewStatus(framework.Success))

	output := tCtx.Logger().GetSink().(testktesting.Underlier).GetBuffer().String()
	wantSubstrings := []string{
		"Preemption candidate scored for pod",
		"Preemption candidate selected for pod",
		"profile=\"better-scheduler\"",
		"decisionID=\"incoming-uid\"",
		"attempt=9",
		"node=\"node-a\"",
		"score=50",
		"score_name=\"ProtectedGPUPacking\"",
		"incoming_gpu=1",
		"protected_gpu_after_victims=3",
		"selected=true",
		"selection_path=\"preemption\"",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(output, want) {
			t.Fatalf("expected log output to contain %q, got:\n%s", want, output)
		}
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
			name: "same namespace semi-guaranteed cpu pod deleted",
			targetPod: makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{
				v1.ResourceCPU: "1",
			}),
			oldObj: makePodWithRequests("running", "team-a", "semi", "node-a", map[v1.ResourceName]string{
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
		args: tieredArgs(map[string]v1.ResourceList{
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

func TestClassifyPreemptionEventSummarizesLongVictimList(t *testing.T) {
	victims := make([]*v1.Pod, 0, 80)
	for i := 0; i < cap(victims); i++ {
		victims = append(victims, makePodWithRequests(
			fmt.Sprintf("victim-%02d-%s", i, strings.Repeat("x", 32)),
			"team-b",
			"normal",
			"node-a",
			map[v1.ResourceName]string{v1.ResourceCPU: "1"},
		))
	}
	victimKeys := podKeys(victims)

	event := classifyPreemptionEvent(
		makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"}),
		framework.NewPostFilterResultWithNominatedNode("node-a"),
		framework.NewStatus(framework.Success),
		&preemptionDecisionTrace{nodes: map[string]*nodePreemptionTrace{
			"node-a": {
				nodeName: "node-a",
				victims:  victims,
			},
		}},
	)

	if len(event.note) > corevalidation.NoteLengthLimit {
		t.Fatalf("expected note length <= %d, got %d: %q", corevalidation.NoteLengthLimit, len(event.note), event.note)
	}
	if !strings.Contains(event.note, "phase=started") {
		t.Fatalf("expected started phase in note, got %q", event.note)
	}
	if !strings.Contains(event.note, "nominatedNode=node-a") {
		t.Fatalf("expected nominated node in note, got %q", event.note)
	}
	if !strings.Contains(event.note, victimKeys[0]) {
		t.Fatalf("expected first victim key in note, got %q", event.note)
	}
	if !strings.Contains(event.note, "...(+") {
		t.Fatalf("expected summarized victim list in note, got %q", event.note)
	}
	if strings.Contains(event.note, victimKeys[len(victimKeys)-1]) {
		t.Fatalf("expected note to omit at least one trailing victim, got %q", event.note)
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

	plugin.logPreemptionDecision(context.Background(), framework.NewCycleState(), pod, nil, framework.NewStatus(framework.Unschedulable, preemptionWaitingOnTerminatingVictims))

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
	resetMetricsForTest()
	t.Cleanup(resetMetricsForTest)
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
			expected := `
				# HELP scheduler_namespace_resource_guarantee_victim_deletions_total [ALPHA] Number of successful preemption victim deletion requests initiated by NamespaceResourceGuarantee.
				# TYPE scheduler_namespace_resource_guarantee_victim_deletions_total counter
				scheduler_namespace_resource_guarantee_victim_deletions_total{namespace="team-a",profile="",tier="guaranteed"} 1
			`
			if err := testutil.GatherAndCompare(
				metrics.GetGather(),
				strings.NewReader(expected),
				"scheduler_namespace_resource_guarantee_victim_deletions_total",
			); err != nil {
				t.Fatal(err)
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

func tieredArgs(guarantees map[string]v1.ResourceList) config.NamespaceResourceGuaranteeArgs {
	args := newArgs(guarantees)
	args.SemiProtectedPriorityClassName = "semi"
	args.AdmissionAssignedTierNamespaces = []string{"team-a"}
	return args
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

type errorSharedLister struct{}

func (errorSharedLister) NodeInfos() framework.NodeInfoLister {
	return errorNodeInfoLister{}
}

func (errorSharedLister) StorageInfos() framework.StorageInfoLister {
	return nil
}

type errorNodeInfoLister struct{}

func (errorNodeInfoLister) List() ([]*framework.NodeInfo, error) {
	return nil, errors.New("snapshot list failed")
}

func (errorNodeInfoLister) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) {
	return nil, errors.New("snapshot list failed")
}

func (errorNodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return nil, errors.New("snapshot list failed")
}

func (errorNodeInfoLister) Get(string) (*framework.NodeInfo, error) {
	return nil, errors.New("snapshot get failed")
}

func TestSyncNominatedNodeReservationLifecycle(t *testing.T) {
	nominatednodereservation.ResetSharedStoreForTest()
	t.Cleanup(nominatednodereservation.ResetSharedStoreForTest)

	recorder := clientgoevents.NewFakeRecorder(4)
	fh, err := frameworkruntime.NewFramework(context.Background(), nil, nil, frameworkruntime.WithEventRecorder(recorder))
	if err != nil {
		t.Fatalf("Failed creating framework runtime: %v", err)
	}

	plugin := &NamespaceResourceGuarantee{handle: fh}
	preemptor := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{v1.ResourceCPU: "1"})
	result := framework.NewPostFilterResultWithNominatedNode("node-a")

	plugin.syncNominatedNodeReservation(context.Background(), preemptor, result, framework.NewStatus(framework.Success))

	reservation, ok := nominatednodereservation.SharedStore().Get("node-a")
	if !ok {
		t.Fatal("expected reservation to exist after successful nomination")
	}
	if reservation.HolderPodUID != preemptor.UID {
		t.Fatalf("unexpected holder UID %q", reservation.HolderPodUID)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, nominatednodereservation.EventReasonNodeReserved) {
			t.Fatalf("expected reserve event reason in event, got %q", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reserve event")
	}

	plugin.syncNominatedNodeReservation(
		context.Background(),
		preemptor,
		nil,
		framework.NewStatus(framework.Unschedulable, preemptionNoCandidateMessage),
	)

	if _, ok := nominatednodereservation.SharedStore().Get("node-a"); ok {
		t.Fatal("expected reservation to be released when no preemption candidate is available")
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-recorder.Events:
			if strings.Contains(event, nominatednodereservation.EventReasonNodeReservationReleased) {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for reservation release event")
		}
	}
}
