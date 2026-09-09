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

package v1

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	configv1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

func TestNamespaceResourceGuaranteeArgsConversionPreservesManagedVictimRestriction(t *testing.T) {
	scheme := GetPluginArgConversionScheme()
	for _, tt := range []struct {
		name    string
		classes []string
	}{
		{name: "omitted"},
		{name: "empty", classes: []string{}},
		{name: "guaranteed", classes: []string{"protected"}},
		{name: "semi-guaranteed", classes: []string{"semi"}},
		{name: "both", classes: []string{"protected", "semi"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			external := &configv1.NamespaceResourceGuaranteeArgs{
				ProtectedPriorityClassName:            "protected",
				SemiProtectedPriorityClassName:        "semi",
				AdmissionAssignedTierNamespaces:       []string{"team-a"},
				RestrictPreemptionToManagedNamespaces: tt.classes,
				NamespaceGuarantees: map[string]corev1.ResourceList{
					"team-a": {corev1.ResourceCPU: resource.MustParse("1")},
				},
			}
			internal := &config.NamespaceResourceGuaranteeArgs{}
			if err := scheme.Convert(external, internal, nil); err != nil {
				t.Fatalf("converting NamespaceResourceGuaranteeArgs to internal: %v", err)
			}
			if diff := cmp.Diff(tt.classes, internal.RestrictPreemptionToManagedNamespaces); diff != "" {
				t.Fatalf("internal restriction (-want,+got):\n%s", diff)
			}

			roundTripped := &configv1.NamespaceResourceGuaranteeArgs{}
			if err := scheme.Convert(internal, roundTripped, nil); err != nil {
				t.Fatalf("converting NamespaceResourceGuaranteeArgs to v1: %v", err)
			}
			if diff := cmp.Diff(external, roundTripped); diff != "" {
				t.Fatalf("round-tripped args (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestNamespaceResourceGuaranteeRDMAConversion(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		external := &configv1.NamespaceResourceGuaranteeArgs{PreferNonRDMANodesForGuaranteedGPU: enabled}
		internal := &config.NamespaceResourceGuaranteeArgs{}
		scheme := GetPluginArgConversionScheme()
		if err := scheme.Convert(external, internal, nil); err != nil {
			t.Fatal(err)
		}
		if internal.PreferNonRDMANodesForGuaranteedGPU != enabled {
			t.Fatal("internal conversion lost RDMA preference")
		}
		roundTrip := &configv1.NamespaceResourceGuaranteeArgs{}
		if err := scheme.Convert(internal, roundTrip, nil); err != nil {
			t.Fatal(err)
		}
		if roundTrip.PreferNonRDMANodesForGuaranteedGPU != enabled {
			t.Fatal("v1 conversion lost RDMA preference")
		}
	}
}
