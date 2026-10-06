/*
Copyright 2014 The Kubernetes Authors.

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

package scheduler

import (
	"context"
	"fmt"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/namespaceresourceguarantee"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nominatednodereservation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

const testGPU v1.ResourceName = "nvidia.com/gpu"
const testRDMA v1.ResourceName = "nvidia.com/rdma_shared_device_a"

type rdmaFixture struct {
	pod       *v1.Pod
	nodes     []*v1.Node
	pods      []*v1.Pod
	pdbs      []*policyv1.PodDisruptionBudget
	args      config.NamespaceResourceGuaranteeArgs
	extenders []framework.Extender
}

func rdmaPod(name, node string, priority int32) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID(name), Labels: map[string]string{"pdb": node}},
		Spec: v1.PodSpec{NodeName: node, SchedulerName: "better-scheduler", Priority: &priority,
			Containers: []v1.Container{{Name: "work", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{testGPU: resource.MustParse("1")}}}},
		},
	}
}

func rdmaNode(name string, rdma bool) *v1.Node {
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"name": name}}, Status: v1.NodeStatus{Allocatable: v1.ResourceList{
		testGPU: resource.MustParse("1"), v1.ResourceCPU: resource.MustParse("128"), v1.ResourceMemory: resource.MustParse("1Ti"), v1.ResourcePods: resource.MustParse("100"),
	}}}
	if rdma {
		node.Status.Allocatable[testRDMA] = resource.MustParse("32")
	}
	node.Status.Capacity = node.Status.Allocatable.DeepCopy()
	return node
}

func blockPDB(node string) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: node, Namespace: "team-a"},
		Spec:   policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"pdb": node}}},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0},
	}
}

func newRDMAFixture() *rdmaFixture {
	pod := rdmaPod("incoming", "", 1000)
	pod.Spec.PriorityClassName = "guaranteed"
	return &rdmaFixture{pod: pod, nodes: []*v1.Node{rdmaNode("node1", false), rdmaNode("node2", true)},
		args: config.NamespaceResourceGuaranteeArgs{ProtectedPriorityClassName: "guaranteed", PreferNonRDMANodesForGuaranteedGPU: true,
			NamespaceGuarantees: map[string]v1.ResourceList{"team-a": {testGPU: resource.MustParse("1000")}}},
	}
}

func startRDMAFixture(t *testing.T, f *rdmaFixture) (*Scheduler, framework.Framework, *clientsetfake.Clientset, context.Context) {
	t.Helper()
	metrics.Register()
	nominatednodereservation.ResetSharedStoreForTest()
	t.Cleanup(nominatednodereservation.ResetSharedStoreForTest)
	logger, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	objects := []runtime.Object{f.pod}
	for _, pod := range f.pods {
		objects = append(objects, pod)
	}
	for _, pdb := range f.pdbs {
		objects = append(objects, pdb)
	}
	cs := clientsetfake.NewClientset(objects...)
	factory := informers.NewSharedInformerFactory(cs, 0)
	for _, pod := range append([]*v1.Pod{f.pod}, f.pods...) {
		if err := factory.Core().V1().Pods().Informer().GetStore().Add(pod); err != nil {
			t.Fatal(err)
		}
	}
	for _, pdb := range f.pdbs {
		if err := factory.Policy().V1().PodDisruptionBudgets().Informer().GetStore().Add(pdb); err != nil {
			t.Fatal(err)
		}
	}
	cache := internalcache.New(ctx, 0)
	for _, node := range f.nodes {
		cache.AddNode(logger, node)
	}
	for _, pod := range f.pods {
		if err := cache.AddPod(logger, pod); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := internalcache.NewEmptySnapshot()
	fwk, err := tf.NewFramework(ctx, []tf.RegisterPluginFunc{
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterPluginAsExtensions(noderesources.Name, frameworkruntime.FactoryAdapter(feature.Features{}, noderesources.NewFit), "PreFilter", "Filter"),
		tf.RegisterPluginAsExtensions(nodeaffinity.Name, frameworkruntime.FactoryAdapter(feature.Features{}, nodeaffinity.New), "PreFilter", "Filter"),
		tf.RegisterPluginAsExtensions(namespaceresourceguarantee.Name, func(ctx context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
			return namespaceresourceguarantee.New(ctx, &f.args, handle)
		}, "PreFilter", "Filter", "PostFilter"),
		tf.RegisterPluginAsExtensions(nominatednodereservation.Name, nominatednodereservation.New, "Filter", "PostBind"),
		tf.RegisterScorePlugin("Node2Prioritizer", tf.NewNode2PrioritizerPlugin(), 100),
	}, "better-scheduler", frameworkruntime.WithClientSet(cs), frameworkruntime.WithInformerFactory(factory),
		frameworkruntime.WithEventRecorder(events.NewFakeRecorder(10000)), frameworkruntime.WithSnapshotSharedLister(snapshot),
		frameworkruntime.WithPodNominator(internalqueue.NewSchedulingQueue(nil, factory)), frameworkruntime.WithWaitingPods(frameworkruntime.NewWaitingPodsMap()),
		frameworkruntime.WithExtenders(f.extenders), frameworkruntime.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fwk.Close() })
	sched := &Scheduler{Cache: cache, nodeInfoSnapshot: snapshot, percentageOfNodesToScore: 1, Extenders: f.extenders}
	sched.applyDefaultHandlers()
	if f.pod.Status.NominatedNodeName != "" {
		nominatednodereservation.SharedStore().Reserve(f.pod.Status.NominatedNodeName, f.pod, namespaceresourceguarantee.Name)
	}
	return sched, fwk, cs, ctx
}

func deletedPods(cs *clientsetfake.Clientset) []string {
	var deleted []string
	for _, action := range cs.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			deleted = append(deleted, action.(interface{ GetName() string }).GetName())
		}
	}
	sort.Strings(deleted)
	return deleted
}

func TestRDMAPlacementOrder(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, async)
			RDMAPlacementOrder(t)
		})
	}
}

func RDMAPlacementOrder(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(*rdmaFixture)
		host, nominated string
		deleted         []string
		retry           bool
	}{
		{name: "ordinary free beats highest RDMA score", host: "node1"},
		{name: "ordinary safe preemption beats free RDMA", setup: func(f *rdmaFixture) { f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)} }, nominated: "node1", deleted: []string{"ordinary"}},
		{name: "free RDMA when ordinary cannot be preempted", setup: func(f *rdmaFixture) { f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 1000)} }, host: "node2", retry: true},
		{name: "free RDMA preserves ordinary PDB", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
			f.pdbs = []*policyv1.PodDisruptionBudget{blockPDB("node1")}
		}, host: "node2", retry: true},
		{name: "safe ordinary preemption before safe RDMA preemption", setup: func(f *rdmaFixture) { f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0), rdmaPod("rdma", "node2", 0)} }, nominated: "node1", deleted: []string{"ordinary"}},
		{name: "safe RDMA preemption before ordinary PDB violation", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0), rdmaPod("rdma", "node2", 0)}
			f.pdbs = []*policyv1.PodDisruptionBudget{blockPDB("node1")}
		}, nominated: "node2", deleted: []string{"rdma"}, retry: true},
		{name: "equal PDB violations prefer ordinary", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0), rdmaPod("rdma", "node2", 0)}
			f.pdbs = []*policyv1.PodDisruptionBudget{blockPDB("node1"), blockPDB("node2")}
		}, nominated: "node1", deleted: []string{"ordinary"}, retry: true},
		{name: "fewer PDB violations beat ordinary preference", setup: func(f *rdmaFixture) {
			f.nodes[0].Status.Allocatable[testGPU] = resource.MustParse("2")
			f.nodes[1].Status.Allocatable[testGPU] = resource.MustParse("2")
			f.pod.Spec.Containers[0].Resources.Requests[testGPU] = resource.MustParse("2")
			r := rdmaPod("rdma", "node2", 0)
			r.Spec.Containers[0].Resources.Requests[testGPU] = resource.MustParse("2")
			f.pods = []*v1.Pod{rdmaPod("ordinary1", "node1", 0), rdmaPod("ordinary2", "node1", 0), r}
			f.pdbs = []*policyv1.PodDisruptionBudget{blockPDB("node1"), blockPDB("node2")}
		}, nominated: "node2", deleted: []string{"rdma"}, retry: true},
		{name: "no ordinary nodes", setup: func(f *rdmaFixture) { f.nodes = f.nodes[1:] }, host: "node2", retry: true},
		{name: "PreemptNever permits free RDMA fallback", setup: func(f *rdmaFixture) {
			p := v1.PreemptNever
			f.pod.Spec.PreemptionPolicy = &p
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
		}, host: "node2", retry: true},
		{name: "PreemptNever cannot evict on either group", setup: func(f *rdmaFixture) {
			p := v1.PreemptNever
			f.pod.Spec.PreemptionPolicy = &p
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0), rdmaPod("rdma", "node2", 0)}
		}, retry: true},
		{name: "quota failure does not enable fallback", setup: func(f *rdmaFixture) { f.args.NamespaceGuarantees["team-a"][testGPU] = resource.MustParse("0") }},
		{name: "flag disabled preserves scoring", setup: func(f *rdmaFixture) { f.args.PreferNonRDMANodesForGuaranteedGPU = false }, host: "node2"},
		{name: "semi guaranteed preserves scoring", setup: func(f *rdmaFixture) {
			f.pod.Spec.PriorityClassName = "semi"
			f.args.SemiProtectedPriorityClassName = "semi"
			f.args.AdmissionAssignedTierNamespaces = []string{"team-a"}
		}, host: "node2"},
		{name: "CPU only preserves scoring", setup: func(f *rdmaFixture) {
			f.pod.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}
		}, host: "node2"},
		{name: "GPU plus RDMA is unaffected", setup: func(f *rdmaFixture) { f.pod.Spec.Containers[0].Resources.Requests[testRDMA] = resource.MustParse("1") }, host: "node2"},
		{name: "managed namespace restriction preserved", setup: func(f *rdmaFixture) {
			f.args.RestrictPreemptionToManagedNamespaces = []string{f.args.ProtectedPriorityClassName}
			p := rdmaPod("unmanaged", "node1", 0)
			p.Namespace = "unmanaged"
			f.pods = []*v1.Pod{p}
		}, host: "node2", retry: true},

		{name: "filter-only extender rejects ordinary candidate", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
			f.extenders = []framework.Extender{&rdmaFilterOnlyExtender{FakeExtender: tf.FakeExtender{Predicates: []tf.FitPredicate{tf.Node2PredicateExtender}}}}
		}, host: "node2", retry: true},
		{name: "extender cannot introduce higher priority victim", setup: func(f *rdmaFixture) {
			high := rdmaPod("higher-priority", "node1", 2000)
			high.Spec.Containers[0].Resources.Requests = nil
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0), high}
			f.extenders = []framework.Extender{&rdmaAddedVictimExtender{victim: high}}
		}},
		{name: "extender rejects ordinary preemption candidate", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
			f.extenders = []framework.Extender{&tf.FakeExtender{Predicates: []tf.FitPredicate{tf.Node2PredicateExtender}}}
		}, host: "node2", retry: true},
		{name: "extender error is not proof of no ordinary candidate", setup: func(f *rdmaFixture) {
			f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
			f.extenders = []framework.Extender{&tf.FakeExtender{Predicates: []tf.FitPredicate{tf.ErrorPredicateExtender}}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRDMAFixture()
			if tt.setup != nil {
				tt.setup(f)
			}
			sched, fwk, cs, ctx := startRDMAFixture(t, f)
			state := framework.NewCycleState()
			result, status := sched.schedulePodWithPostFilter(ctx, fwk, state, f.pod)
			if (tt.host != "") != status.IsSuccess() {
				t.Fatalf("host=%q status=%v", result.SuggestedHost, status)
			}
			if result.SuggestedHost != tt.host {
				t.Errorf("host=%q, want %q", result.SuggestedHost, tt.host)
			}
			nominated := ""
			if result.nominatingInfo != nil {
				nominated = result.nominatingInfo.NominatedNodeName
			}
			if nominated != tt.nominated {
				t.Errorf("nomination=%q, want %q", nominated, tt.nominated)
			}
			if diff := cmp.Diff(tt.deleted, waitDeletedPods(t, cs, len(tt.deleted))); diff != "" {
				t.Errorf("deleted pods (-want,+got): %s", diff)
			}
			if state.IsSchedulingRetry != tt.retry {
				t.Errorf("retry=%v, want %v", state.IsSchedulingRetry, tt.retry)
			}
			if tt.host != "" {
				for _, action := range cs.Actions() {
					if action.GetVerb() == "patch" || action.GetVerb() == "delete" {
						t.Errorf("unexpected API mutation before direct placement: %v", action)
					}
				}
			}
		})
	}
}

func TestRDMAInFlightPreemption(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, async)
			RDMAInFlightPreemption(t)
		})
	}
}

func RDMAInFlightPreemption(t *testing.T) {
	tests := []struct {
		name, nomination              string
		normalBusy, rdmaBusy, invalid bool
		host, newNomination           string
	}{
		{name: "wait for ordinary victims instead of free RDMA", nomination: "node1", normalBusy: true},
		{name: "wait for RDMA victims instead of second preemption", nomination: "node2", normalBusy: true, rdmaBusy: true},
		{name: "finish ready RDMA nomination without second preemption", nomination: "node2", normalBusy: true, host: "node2"},
		{name: "use newly free ordinary node", nomination: "node2", rdmaBusy: true, host: "node1"},
		{name: "invalid RDMA nomination restarts ordinary search", nomination: "node2", normalBusy: true, rdmaBusy: true, invalid: true, newNomination: "node1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRDMAFixture()
			f.pod.Status.NominatedNodeName = tt.nomination
			for _, item := range []struct {
				node string
				busy bool
			}{{"node1", tt.normalBusy}, {"node2", tt.rdmaBusy}} {
				if !item.busy {
					continue
				}
				p := rdmaPod(item.node+"-victim", item.node, 0)
				if item.node == tt.nomination {
					now := metav1.Now()
					p.DeletionTimestamp = &now
					p.Status.Conditions = []v1.PodCondition{{Type: v1.DisruptionTarget, Status: v1.ConditionTrue, Reason: v1.PodReasonPreemptionByScheduler}}
				}
				f.pods = append(f.pods, p)
			}
			if tt.invalid {
				f.pod.Spec.NodeSelector = map[string]string{"name": "node1"}
			}
			sched, fwk, cs, ctx := startRDMAFixture(t, f)
			result, status := sched.schedulePodWithPostFilter(ctx, fwk, framework.NewCycleState(), f.pod)
			if result.SuggestedHost != tt.host {
				t.Fatalf("host=%q want=%q status=%v", result.SuggestedHost, tt.host, status)
			}
			var wantDeleted []string
			if tt.newNomination != "" {
				wantDeleted = []string{"node1-victim"}
				if result.nominatingInfo == nil || result.nominatingInfo.NominatedNodeName != tt.newNomination {
					t.Fatalf("unexpected nomination: %+v", result.nominatingInfo)
				}
			}
			if diff := cmp.Diff(wantDeleted, waitDeletedPods(t, cs, len(wantDeleted))); diff != "" {
				t.Fatal(diff)
			}
			if tt.host == "" && tt.newNomination == "" {
				if !strings.Contains(status.Message(), "terminating") {
					t.Fatalf("expected waiting: %v", status)
				}
				if reservation, ok := nominatednodereservation.SharedStore().Get(tt.nomination); !ok || reservation.HolderPodUID != f.pod.UID {
					t.Fatal("lost in-flight reservation")
				}
			}
			if tt.host != "" {
				bound := f.pod.DeepCopy()
				bound.Spec.NodeName = tt.host
				fwk.RunPostBindPlugins(ctx, framework.NewCycleState(), bound, tt.host)
				if _, ok := nominatednodereservation.SharedStore().Get(tt.nomination); ok {
					t.Fatal("reservation retained after binding")
				}
			}
		})
	}
}

func TestRDMAExhaustiveSearch(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SchedulerAsyncPreemption, async)
			RDMAExhaustiveSearch(t)
		})
	}
}

func RDMAExhaustiveSearch(t *testing.T) {
	for _, preempt := range []bool{false, true} {
		t.Run(fmt.Sprintf("preemption=%v", preempt), func(t *testing.T) {
			f := newRDMAFixture()
			f.nodes = nil
			for i := 0; i < 220; i++ {
				name := fmt.Sprintf("ordinary-%03d", i)
				f.nodes = append(f.nodes, rdmaNode(name, false))
				if preempt {
					f.pods = append(f.pods, rdmaPod(name+"-victim", name, 0))
				}
			}
			f.nodes = append(f.nodes, rdmaNode("node2", true))
			// Every native candidate fits; the extender permits only the last ordinary
			// node. A 100-node native shortlist must not cause an RDMA fallback.
			f.extenders = []framework.Extender{&tf.FakeExtender{Predicates: []tf.FitPredicate{func(_ *v1.Pod, node *framework.NodeInfo) *framework.Status {
				if node.Node().Name == "ordinary-219" || node.Node().Name == "node2" {
					return nil
				}
				return framework.NewStatus(framework.Unschedulable, "extender rejects node")
			}}}}
			sched, fwk, cs, ctx := startRDMAFixture(t, f)
			state := framework.NewCycleState()
			result, status := sched.schedulePodWithPostFilter(ctx, fwk, state, f.pod)
			if preempt {
				if result.nominatingInfo == nil || result.nominatingInfo.NominatedNodeName != "ordinary-219" {
					t.Fatalf("wrong nomination: %+v, %v", result, status)
				}
				if diff := cmp.Diff([]string{"ordinary-219-victim"}, waitDeletedPods(t, cs, 1)); diff != "" {
					t.Fatal(diff)
				}
			} else if !status.IsSuccess() || result.SuggestedHost != "ordinary-219" {
				t.Fatalf("wrong host: %+v, %v", result, status)
			}
			if state.IsSchedulingRetry {
				t.Fatal("unexpected RDMA fallback")
			}
		})
	}
}

// The scheduler may consult PostFilter twice, but must use one cache snapshot.
type rdmaCountingCache struct {
	internalcache.Cache
	updates int
}

func (c *rdmaCountingCache) UpdateSnapshot(logger klog.Logger, snapshot *internalcache.Snapshot) error {
	c.updates++
	return c.Cache.UpdateSnapshot(logger, snapshot)
}

func TestRDMARetryReusesSnapshot(t *testing.T) {
	f := newRDMAFixture()
	f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 1000)}
	sched, fwk, _, ctx := startRDMAFixture(t, f)
	counter := &rdmaCountingCache{Cache: sched.Cache}
	sched.Cache = counter
	state := framework.NewCycleState()
	result, status := sched.schedulePodWithPostFilter(ctx, fwk, state, f.pod)
	if !status.IsSuccess() || result.SuggestedHost != "node2" || !state.IsSchedulingRetry || counter.updates != 1 {
		t.Fatalf("result=%+v status=%v retry=%v snapshots=%d", result, status, state.IsSchedulingRetry, counter.updates)
	}
}

type rdmaRetryFramework struct {
	framework.Framework
	result *framework.PostFilterResult
	status *framework.Status
}

func (f *rdmaRetryFramework) HasPostFilterPlugins() bool { return true }
func (f *rdmaRetryFramework) RunPostFilterPlugins(context.Context, *framework.CycleState, *v1.Pod, framework.NodeToStatusReader) (*framework.PostFilterResult, *framework.Status) {
	return f.result, f.status
}

func TestSchedulingRetryBound(t *testing.T) {
	for _, tt := range []struct {
		name       string
		result     *framework.PostFilterResult
		status     *framework.Status
		secondFits bool
		calls      int
		wantError  bool
	}{
		{name: "one retry", result: &framework.PostFilterResult{RetryScheduling: true}, secondFits: true, calls: 2},
		{name: "repeated retry", result: &framework.PostFilterResult{RetryScheduling: true}, calls: 2, wantError: true},
		{name: "retry with nomination", result: &framework.PostFilterResult{RetryScheduling: true, NominatingInfo: &framework.NominatingInfo{NominatedNodeName: "node1"}}, calls: 1, wantError: true},
		{name: "retry with failure", result: &framework.PostFilterResult{RetryScheduling: true}, status: framework.NewStatus(framework.Unschedulable), calls: 1, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := rdmaPod("incoming", "", 1000)
			calls := 0
			sched := &Scheduler{SchedulePod: func(_ context.Context, _ framework.Framework, state *framework.CycleState, _ *v1.Pod) (ScheduleResult, error) {
				calls++
				if calls == 2 && tt.secondFits {
					return ScheduleResult{SuggestedHost: "node1"}, nil
				}
				return ScheduleResult{}, &framework.FitError{Pod: pod, Diagnosis: framework.Diagnosis{NodeToStatus: framework.NewDefaultNodeToStatus()}}
			}}
			fwk := &rdmaRetryFramework{result: tt.result, status: tt.status}
			_, status := sched.schedulePodWithPostFilter(context.Background(), fwk, framework.NewCycleState(), pod)
			if calls != tt.calls || (status.Code() == framework.Error) != tt.wantError {
				t.Fatalf("calls=%d status=%v", calls, status)
			}
		})
	}
}

type rdmaFilterOnlyExtender struct{ tf.FakeExtender }

func (*rdmaFilterOnlyExtender) SupportsPreemption() bool { return false }

type rdmaAddedVictimExtender struct {
	tf.FakeExtender
	victim *v1.Pod
}

func (e *rdmaAddedVictimExtender) ProcessPreemption(_ *v1.Pod, candidates map[string]*extenderv1.Victims, _ framework.NodeInfoLister) (map[string]*extenderv1.Victims, error) {
	for _, victims := range candidates {
		victims.Pods = append(victims.Pods, e.victim)
	}
	return candidates, nil
}

type rdmaCancelingExtender struct {
	tf.FakeExtender
	cancel context.CancelFunc
}

func (e *rdmaCancelingExtender) ProcessPreemption(_ *v1.Pod, candidates map[string]*extenderv1.Victims, _ framework.NodeInfoLister) (map[string]*extenderv1.Victims, error) {
	e.cancel()
	return candidates, nil
}

func TestRDMACanceledEvaluationDoesNotEvict(t *testing.T) {
	f := newRDMAFixture()
	f.pods = []*v1.Pod{rdmaPod("ordinary", "node1", 0)}
	extender := &rdmaCancelingExtender{}
	f.extenders = []framework.Extender{extender}
	sched, fwk, cs, ctx := startRDMAFixture(t, f)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	extender.cancel = cancel
	state := framework.NewCycleState()
	result, status := sched.schedulePodWithPostFilter(ctx, fwk, state, f.pod)
	if status.IsSuccess() || !strings.Contains(status.Message(), "canceled") || result.nominatingInfo != nil || state.IsSchedulingRetry {
		t.Fatalf("result=%+v status=%v retry=%v", result, status, state.IsSchedulingRetry)
	}
	if got := deletedPods(cs); len(got) != 0 {
		t.Fatalf("evicted after cancellation: %v", got)
	}
}

// Async preemption returns a nomination before the API deletion completes.
func waitDeletedPods(t *testing.T, cs *clientsetfake.Clientset, count int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := deletedPods(cs)
		if len(got) >= count || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Millisecond)
	}
}
