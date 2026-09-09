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
)

const nodeResourcePreferenceKey StateKey = "nodeResourcePreference"

// NodeResourcePreference defers nodes advertising Resource until PostFilter has
// exhausted preferred placements. It also requests exhaustive filtering, so
// sampling and extender rejections cannot hide preferred nodes.
type NodeResourcePreference struct {
	Resource      v1.ResourceName
	AllowFallback bool
	// NodeNames is the immutable intersection of all PreFilter node restrictions.
	NodeNames sets.Set[string]
}

func (s *NodeResourcePreference) Clone() StateData {
	copy := *s
	return &copy
}

func WriteNodeResourcePreference(state *CycleState, preference *NodeResourcePreference) {
	state.Write(nodeResourcePreferenceKey, preference)
}

func NodeResourcePreferenceFromState(state *CycleState) *NodeResourcePreference {
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
