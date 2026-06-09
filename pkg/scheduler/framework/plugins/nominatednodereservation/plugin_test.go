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
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	clientgoevents "k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
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
	if status.Code() != framework.Unschedulable {
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

func newTestPodLister(pods ...*v1.Pod) corelisters.PodLister {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, pod := range pods {
		if err := indexer.Add(pod); err != nil {
			panic(err)
		}
	}
	return corelisters.NewPodLister(indexer)
}
