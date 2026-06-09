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
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestStoreReserveAndReleaseByPod(t *testing.T) {
	store := newStore()
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: types.UID("uid-a")}}

	reservation, changed := store.Reserve("node-a", pod, "test")
	if !changed {
		t.Fatal("expected reserve to change store")
	}
	if reservation.NodeName != "node-a" {
		t.Fatalf("unexpected node name %q", reservation.NodeName)
	}

	if _, changed = store.Reserve("node-a", pod, "test"); changed {
		t.Fatal("expected reserve with same holder to be no-op")
	}

	released, ok := store.ReleaseByPod(pod.UID)
	if !ok {
		t.Fatal("expected release by pod to succeed")
	}
	if released.NodeName != "node-a" {
		t.Fatalf("unexpected released node name %q", released.NodeName)
	}

	if _, ok := store.Get("node-a"); ok {
		t.Fatal("expected reservation to be removed")
	}
}

func TestStoreReplacesOldNodeForSamePod(t *testing.T) {
	store := newStore()
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: types.UID("uid-a")}}

	_, _ = store.Reserve("node-a", pod, "test")
	_, changed := store.Reserve("node-b", pod, "test")
	if !changed {
		t.Fatal("expected move to new node to change store")
	}

	if _, ok := store.Get("node-a"); ok {
		t.Fatal("expected old node reservation to be removed")
	}
	if got, ok := store.Get("node-b"); !ok || got.HolderPodUID != pod.UID {
		t.Fatalf("expected new reservation on node-b for pod %q, got %#v (exists=%v)", pod.UID, got, ok)
	}
}
