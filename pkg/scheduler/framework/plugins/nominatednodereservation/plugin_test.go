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

package nominatednodereservation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	clientgoevents "k8s.io/client-go/tools/events"
	"k8s.io/component-base/metrics/testutil"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	schedmetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestFilterBlocksReservedNodeForNonHolder(t *testing.T) {
	ResetSharedStoreForTest()
	t.Cleanup(ResetSharedStoreForTest)

	holder := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
	_, _ = SharedStore().Reserve("node-a", holder, "test")

	pl := &NominatedNodeReservation{
		store:     SharedStore(),
		podLister: newTestPodLister(holder),
	}

	blockedPod := st.MakePod().Namespace("team-b").Name("blocked").UID("blocked-uid").Obj()
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node-a").Obj())

	_, ctx := ktesting.NewTestContext(t)
	status := pl.Filter(ctx, framework.NewCycleState(), blockedPod, nodeInfo)
	if status == nil {
		t.Fatal("expected pod to be blocked by reservation")
	}
	if status.Code() != fwk.Unschedulable {
		t.Fatalf("unexpected status code %v", status.Code())
	}
	if !strings.Contains(status.Message(), ErrReasonNodeReserved) {
		t.Fatalf("expected reason %q, got %q", ErrReasonNodeReserved, status.Message())
	}
	if !strings.Contains(status.Message(), "team-a/holder") {
		t.Fatalf("expected holder identity in message, got %q", status.Message())
	}
}

func TestFilterAllowsHolderAndCleansStaleReservation(t *testing.T) {
	ResetSharedStoreForTest()
	t.Cleanup(ResetSharedStoreForTest)

	holder := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
	_, _ = SharedStore().Reserve("node-a", holder, "test")

	pl := &NominatedNodeReservation{
		store:     SharedStore(),
		podLister: newTestPodLister(holder),
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node-a").Obj())
	_, ctx := ktesting.NewTestContext(t)

	if status := pl.Filter(ctx, framework.NewCycleState(), holder, nodeInfo); status != nil {
		t.Fatalf("holder pod should pass filter, got %v", status)
	}

	deletingHolder := holder.DeepCopy()
	now := metav1.Now()
	deletingHolder.DeletionTimestamp = &now
	pl.podLister = newTestPodLister(deletingHolder)

	otherPod := st.MakePod().Namespace("team-b").Name("other").UID("other-uid").Obj()
	if status := pl.Filter(ctx, framework.NewCycleState(), otherPod, nodeInfo); status != nil {
		t.Fatalf("expected stale reservation cleanup to allow scheduling, got %v", status)
	}
	if _, ok := SharedStore().Get("node-a"); ok {
		t.Fatal("expected stale reservation to be cleaned")
	}
}

func TestPostBindReleasesReservationAndEmitsEvent(t *testing.T) {
	ResetSharedStoreForTest()
	t.Cleanup(ResetSharedStoreForTest)

	holder := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
	_, _ = SharedStore().Reserve("node-a", holder, "test")

	recorder := clientgoevents.NewFakeRecorder(2)
	fh, err := frameworkruntime.NewFramework(context.Background(), nil, nil, frameworkruntime.WithEventRecorder(recorder))
	if err != nil {
		t.Fatalf("failed creating framework runtime: %v", err)
	}

	pl := &NominatedNodeReservation{handle: fh, store: SharedStore()}
	_, ctx := ktesting.NewTestContext(t)
	pl.PostBind(ctx, framework.NewCycleState(), holder, "node-a")

	if _, ok := SharedStore().Get("node-a"); ok {
		t.Fatal("expected reservation to be released on bind")
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, EventReasonNodeReservationReleased) {
			t.Fatalf("expected release event reason in event, got %q", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for release event")
	}
}

func TestReservationMetrics(t *testing.T) {
	resetMetricsForTest()
	ResetSharedStoreForTest()
	t.Cleanup(func() {
		resetMetricsForTest()
		ResetSharedStoreForTest()
	})
	RegisterMetrics()

	holder := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
	reservation, changed := SharedStore().Reserve("node-a", holder, "test")
	if !changed {
		t.Fatal("expected initial reservation to change store")
	}
	RecordTransition(transitionActionReserved, transitionReasonPreemptionStarted)
	if _, changed := SharedStore().Reserve("node-a", holder, "test"); changed {
		t.Fatal("expected idempotent reservation to leave metrics unchanged")
	}

	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(st.MakeNode().Name("node-a").Obj())
	blockedPod := st.MakePod().Namespace("team-b").Name("blocked").UID("blocked-uid").Obj()
	_, ctx := ktesting.NewTestContext(t)
	for _, profile := range []string{"better-scheduler", "default-better-scheduler"} {
		pl := &NominatedNodeReservation{
			profile:   profile,
			store:     SharedStore(),
			podLister: newTestPodLister(holder),
		}
		if status := pl.Filter(ctx, framework.NewCycleState(), blockedPod, nodeInfo); status == nil || status.Code() != fwk.Unschedulable {
			t.Fatalf("expected profile %q to block pod, got %v", profile, status)
		}
	}

	bindPlugin := &NominatedNodeReservation{store: SharedStore()}
	bindPlugin.PostBind(ctx, framework.NewCycleState(), holder, reservation.NodeName)

	staleHolder := st.MakePod().Namespace("team-a").Name("stale").UID("stale-uid").Obj()
	if _, changed := SharedStore().Reserve("node-b", staleHolder, "test"); !changed {
		t.Fatal("expected stale reservation to change store")
	}
	RecordTransition(transitionActionReserved, transitionReasonPreemptionStarted)
	deletingHolder := staleHolder.DeepCopy()
	now := metav1.Now()
	deletingHolder.DeletionTimestamp = &now
	staleNodeInfo := framework.NewNodeInfo()
	staleNodeInfo.SetNode(st.MakeNode().Name("node-b").Obj())
	cleanupPlugin := &NominatedNodeReservation{
		store:     SharedStore(),
		podLister: newTestPodLister(deletingHolder),
	}
	if status := cleanupPlugin.Filter(ctx, framework.NewCycleState(), blockedPod, staleNodeInfo); status != nil {
		t.Fatalf("expected stale cleanup to allow pod, got %v", status)
	}

	expected := `
		# HELP scheduler_nominated_node_reservation_blocked_pods_total [ALPHA] Number of scheduling attempts blocked to protect a nominated-node reservation.
		# TYPE scheduler_nominated_node_reservation_blocked_pods_total counter
		scheduler_nominated_node_reservation_blocked_pods_total{profile="better-scheduler"} 1
		scheduler_nominated_node_reservation_blocked_pods_total{profile="default-better-scheduler"} 1
		# HELP scheduler_nominated_node_reservation_transitions_total [ALPHA] Number of nominated-node reservation lifecycle transitions.
		# TYPE scheduler_nominated_node_reservation_transitions_total counter
		scheduler_nominated_node_reservation_transitions_total{action="cleaned",reason="holder_terminating"} 1
		scheduler_nominated_node_reservation_transitions_total{action="released",reason="holder_bound"} 1
		scheduler_nominated_node_reservation_transitions_total{action="reserved",reason="preemption_started"} 2
		# HELP scheduler_nominated_node_reservations_active [ALPHA] Number of active process-local nominated-node reservations.
		# TYPE scheduler_nominated_node_reservations_active gauge
		scheduler_nominated_node_reservations_active 0
	`
	if err := testutil.GatherAndCompare(
		schedmetrics.GetGather(),
		strings.NewReader(expected),
		"scheduler_nominated_node_reservation_blocked_pods_total",
		"scheduler_nominated_node_reservation_transitions_total",
		"scheduler_nominated_node_reservations_active",
	); err != nil {
		t.Fatal(err)
	}
}

func TestIsStaleReservationReason(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name        string
		holder      *v1.Pod
		reservation Reservation
		nodeName    string
		wantReason  string
	}{
		{
			name:        "uid changed",
			holder:      st.MakePod().Namespace("team-a").Name("holder").UID("new-uid").Obj(),
			reservation: Reservation{HolderPodUID: "old-uid"},
			nodeName:    "node-a",
			wantReason:  transitionReasonHolderUIDChanged,
		},
		{
			name: "holder terminating",
			holder: func() *v1.Pod {
				pod := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
				pod.DeletionTimestamp = &now
				return pod
			}(),
			reservation: Reservation{HolderPodUID: "holder-uid"},
			nodeName:    "node-a",
			wantReason:  transitionReasonHolderTerminating,
		},
		{
			name:        "holder already bound",
			holder:      st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Node("node-b").Obj(),
			reservation: Reservation{HolderPodUID: "holder-uid"},
			nodeName:    "node-a",
			wantReason:  transitionReasonHolderAlreadyBound,
		},
		{
			name: "nomination moved",
			holder: func() *v1.Pod {
				pod := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
				pod.Status.NominatedNodeName = "node-b"
				return pod
			}(),
			reservation: Reservation{HolderPodUID: "holder-uid"},
			nodeName:    "node-a",
			wantReason:  transitionReasonNominationMoved,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stale, reason, detail := isStaleReservation(tt.holder, tt.reservation, tt.nodeName)
			if !stale {
				t.Fatal("expected reservation to be stale")
			}
			if reason != tt.wantReason {
				t.Fatalf("unexpected reason: got %q, want %q", reason, tt.wantReason)
			}
			if detail == "" {
				t.Fatal("expected operator-facing cleanup detail")
			}
		})
	}
}

func newTestPodLister(pods ...*v1.Pod) corelisters.PodLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		if err := indexer.Add(pod); err != nil {
			panic(err)
		}
	}
	return corelisters.NewPodLister(indexer)
}

func TestDryRunDoesNotCleanStaleReservation(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing=%v", missing), func(t *testing.T) {
			ResetSharedStoreForTest()
			t.Cleanup(ResetSharedStoreForTest)
			holder := st.MakePod().Namespace("team-a").Name("holder").UID("holder-uid").Obj()
			reservation, _ := SharedStore().Reserve("node-a", holder, "test")
			deleting := holder.DeepCopy()
			now := metav1.Now()
			deleting.DeletionTimestamp = &now
			pl := &NominatedNodeReservation{store: SharedStore(), podLister: newTestPodLister(deleting)}
			if missing {
				pl.podLister = newTestPodLister()
			}
			node := framework.NewNodeInfo()
			node.SetNode(st.MakeNode().Name("node-a").Obj())
			other := st.MakePod().Namespace("team-b").Name("other").UID("other-uid").Obj()
			_, ctx := ktesting.NewTestContext(t)
			probe := framework.NewCycleState()
			framework.SetPreemptionDryRun(probe, true)
			if status := pl.Filter(ctx, probe, other, node); !status.IsSuccess() {
				t.Fatal(status)
			}
			if got, ok := SharedStore().Get("node-a"); !ok || got != reservation {
				t.Fatal("dry run mutated reservation")
			}
			if status := pl.Filter(ctx, framework.NewCycleState(), other, node); !status.IsSuccess() {
				t.Fatal(status)
			}
			if _, ok := SharedStore().Get("node-a"); ok {
				t.Fatal("real filter did not clean stale reservation")
			}
		})
	}
}
