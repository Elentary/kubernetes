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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
)

const nodeResourcePreferenceKey fwk.StateKey = "nodeResourcePreference"

// NodeResourcePreference defers nodes advertising Resource until PostFilter has
// exhausted preferred placements. It also requests exhaustive filtering, so
// sampling and extender rejections cannot hide preferred nodes.
type NodeResourcePreference struct {
	Resource      v1.ResourceName
	AllowFallback bool
	// NodeNames is the immutable intersection of all PreFilter node restrictions.
	NodeNames sets.Set[string]
}

func (s *NodeResourcePreference) Clone() fwk.StateData {
	copy := *s
	return &copy
}

func WriteNodeResourcePreference(state fwk.CycleState, preference *NodeResourcePreference) {
	state.Write(nodeResourcePreferenceKey, preference)
}

func NodeResourcePreferenceFromState(state fwk.CycleState) *NodeResourcePreference {
	if state == nil {
		return nil
	}
	data, err := state.Read(nodeResourcePreferenceKey)
	if err != nil {
		return nil
	}
	preference, _ := data.(*NodeResourcePreference)
	return preference
}

// NodeHasResource checks advertised capacity, not the currently unused amount.
// Capacity preserves the classification when devices temporarily become unhealthy.
func NodeHasResource(node *v1.Node, name v1.ResourceName) bool {
	if node == nil {
		return false
	}
	capacity, allocatable := node.Status.Capacity[name], node.Status.Allocatable[name]
	return capacity.Sign() > 0 || allocatable.Sign() > 0
}

const cycleFlagsKey fwk.StateKey = "betterSchedulerCycleFlags"

type cycleFlags struct {
	schedulingRetry  bool
	preemptionDryRun bool
}

func (f *cycleFlags) Clone() fwk.StateData { copy := *f; return &copy }
func flagsFromState(state fwk.CycleState) cycleFlags {
	if state != nil {
		if data, err := state.Read(cycleFlagsKey); err == nil {
			if flags, ok := data.(*cycleFlags); ok {
				return *flags
			}
		}
	}
	return cycleFlags{}
}

// IsSchedulingRetry reports whether the bounded RDMA fallback must reuse its snapshot.
func IsSchedulingRetry(state fwk.CycleState) bool { return flagsFromState(state).schedulingRetry }
func SetSchedulingRetry(state fwk.CycleState, retry bool) {
	f := flagsFromState(state)
	f.schedulingRetry = retry
	state.Write(cycleFlagsKey, &f)
}

// IsPreemptionDryRun prevents reservation mutations during speculative victim evaluation.
func IsPreemptionDryRun(state fwk.CycleState) bool { return flagsFromState(state).preemptionDryRun }
func SetPreemptionDryRun(state fwk.CycleState, dryRun bool) {
	f := flagsFromState(state)
	f.preemptionDryRun = dryRun
	state.Write(cycleFlagsKey, &f)
}
