/*
Copyright 2019 The Kubernetes Authors.

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

package framework

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeResourcePreference(t *testing.T) {
	const rdma v1.ResourceName = "nvidia.com/rdma_shared_device_a"
	for _, tt := range []struct {
		name, capacity, allocatable string
		want                        bool
	}{
		{"absent", "", "", false}, {"zero", "0", "0", false}, {"allocatable", "", "1", true},
		{"capacity", "1", "", true}, {"unhealthy devices", "1", "0", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := &v1.Node{Status: v1.NodeStatus{Capacity: v1.ResourceList{}, Allocatable: v1.ResourceList{}}}
			if tt.capacity != "" {
				node.Status.Capacity[rdma] = resource.MustParse(tt.capacity)
			}
			if tt.allocatable != "" {
				node.Status.Allocatable[rdma] = resource.MustParse(tt.allocatable)
			}
			if got := NodeHasResource(node, rdma); got != tt.want {
				t.Fatalf("got=%v want=%v", got, tt.want)
			}
		})
	}
	state := NewCycleState()
	SetSchedulingRetry(state, true)
	WriteNodeResourcePreference(state, &NodeResourcePreference{Resource: rdma})
	clone := state.Clone()
	SetPreemptionDryRun(clone, true)
	NodeResourcePreferenceFromState(clone).AllowFallback = true
	if !IsSchedulingRetry(clone) || IsPreemptionDryRun(state) || NodeResourcePreferenceFromState(state).AllowFallback {
		t.Fatal("cloning must preserve retry state and isolate speculative changes")
	}
}
