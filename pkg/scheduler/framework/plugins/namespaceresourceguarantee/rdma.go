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
	"fmt"

	v1 "k8s.io/api/core/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
)

const (
	rdmaResource       v1.ResourceName    = "nvidia.com/rdma_shared_device_a"
	rdmaCycleKey       framework.StateKey = "NamespaceResourceGuarantee/RDMA"
	rdmaDeferredReason                    = "RDMA node deferred until ordinary placements are exhausted"
)

var _ framework.FilterPlugin = &NamespaceResourceGuarantee{}
var _ framework.PluginConfigurationValidator = &NamespaceResourceGuarantee{}

type rdmaCycleState struct {
	// Candidate slices are immutable during parallel probes. The trace has its
	// own lock. Only serial PostFilter code replaces these fields.
	ordinary []preemption.Candidate
	trace    *preemptionDecisionTrace
}

func (s *rdmaCycleState) Clone() framework.StateData {
	copy := *s
	return &copy
}

func rdmaStateFromCycle(state *framework.CycleState) *rdmaCycleState {
	data, err := state.Read(rdmaCycleKey)
	if err != nil {
		return nil
	}
	cycle, _ := data.(*rdmaCycleState)
	return cycle
}

func (pl *NamespaceResourceGuarantee) prefersNonRDMA(pod *v1.Pod) bool {
	return pl.args.PreferNonRDMANodesForGuaranteedGPU && pl.podTier(pod) == guaranteedTier &&
		pl.resourceRequest(pod, protectedGPUResource) > 0 && pl.resourceRequest(pod, rdmaResource) == 0
}

// PreFilter enforces the quota before enabling the placement preference. A retry
// retains the phase and evaluated candidates; a new scheduling cycle starts fresh.
func (pl *NamespaceResourceGuarantee) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	result, status := pl.preFilterQuota(ctx, state, pod)
	if status.IsSuccess() && pl.prefersNonRDMA(pod) && framework.NodeResourcePreferenceFromState(state) == nil {
		framework.WriteNodeResourcePreference(state, &framework.NodeResourcePreference{Resource: rdmaResource})
		state.Write(rdmaCycleKey, &rdmaCycleState{})
	}
	return result, status
}

func (pl *NamespaceResourceGuarantee) Filter(_ context.Context, state *framework.CycleState, _ *v1.Pod, node *framework.NodeInfo) *framework.Status {
	preference := framework.NodeResourcePreferenceFromState(state)
	if pl.args.PreferNonRDMANodesForGuaranteedGPU && preference != nil && !preference.AllowFallback && framework.NodeHasResource(node.Node(), preference.Resource) {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, rdmaDeferredReason)
	}
	return nil
}

func (pl *NamespaceResourceGuarantee) ValidatePluginConfiguration(plugins *config.Plugins) error {
	if !pl.args.PreferNonRDMANodesForGuaranteedGPU {
		return nil
	}
	contains := func(set config.PluginSet, name string) bool {
		for _, plugin := range set.Enabled {
			if plugin.Name == name {
				return true
			}
		}
		return false
	}
	for _, point := range []struct {
		name string
		set  config.PluginSet
	}{
		{"preFilter", plugins.PreFilter}, {"filter", plugins.Filter}, {"postFilter", plugins.PostFilter},
	} {
		if !contains(point.set, Name) {
			return fmt.Errorf("preferNonRDMANodesForGuaranteedGPU requires %s in %s", Name, point.name)
		}
	}
	if contains(plugins.PostFilter, "DefaultPreemption") {
		return fmt.Errorf("preferNonRDMANodesForGuaranteedGPU requires DefaultPreemption to be disabled")
	}
	return nil
}

func (pl *NamespaceResourceGuarantee) postFilterRDMA(ctx context.Context, state *framework.CycleState, pod *v1.Pod, statuses framework.NodeToStatusReader) (*framework.PostFilterResult, *framework.Status) {
	preference := framework.NodeResourcePreferenceFromState(state)
	cycle := rdmaStateFromCycle(state)
	if cycle == nil {
		return nil, framework.NewStatus(framework.Error, "missing RDMA cycle state")
	}
	current, err := pl.evaluator.PodLister.Pods(pod.Namespace).Get(pod.Name)
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	if current.UID != pod.UID || !pl.prefersNonRDMA(current) {
		return nil, framework.NewStatus(framework.Error, "preemptor changed during placement evaluation")
	}
	pod = current
	never := pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever
	if !never && pod.Status.NominatedNodeName != "" {
		ready, waiting, err := pl.nominatedPlacement(ctx, state, pod)
		if err != nil {
			return nil, framework.AsStatus(err)
		}
		if waiting || (ready && preference.AllowFallback) {
			return nil, framework.NewStatus(framework.Unschedulable, preemptionWaitingOnTerminatingVictims)
		}
		if ready {
			return pl.allowRDMA(ctx, state, pod, "finish_existing_nomination")
		}
	}
	if never {
		if !preference.AllowFallback {
			return pl.allowRDMA(ctx, state, pod, "preemption_disabled")
		}
		return framework.NewPostFilterResultWithNominatedNode(""), framework.NewStatus(framework.Unschedulable, "not eligible due to preemptionPolicy=Never.")
	}

	potential, err := statuses.NodesForStatusCode(pl.handle.SnapshotSharedLister().NodeInfos(), framework.Unschedulable)
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	nodes := make([]*framework.NodeInfo, 0, len(potential))
	for _, node := range potential {
		// First evaluate ordinary nodes; the fallback evaluates only RDMA nodes.
		if framework.NodeHasResource(node.Node(), rdmaResource) == preference.AllowFallback {
			nodes = append(nodes, node)
		}
	}
	candidates, err := pl.evaluator.EvaluateCandidates(ctx, state, pod, nodes)
	if err != nil {
		return nil, framework.AsStatus(err)
	}
	// Extenders may add victims. They must respect the same priority and
	// managed-namespace eligibility as the native victim selector.
	for _, candidate := range candidates {
		for _, victim := range candidate.Victims().Pods {
			if corev1helpers.PodPriority(victim) >= corev1helpers.PodPriority(pod) || !pl.isEligiblePreemptionVictim(pod, victim) {
				return nil, framework.NewStatus(framework.Error, "extender returned an ineligible preemption victim")
			}
		}
	}
	if !preference.AllowFallback {
		cycle.ordinary = candidates
		safe := make([]preemption.Candidate, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.Victims().NumPDBViolations == 0 {
				safe = append(safe, candidate)
			}
		}
		if len(safe) == 0 {
			reason := "no_ordinary_preemption_candidate"
			if len(candidates) > 0 {
				reason = "ordinary_preemption_would_violate_pdb"
			}
			return pl.allowRDMA(ctx, state, pod, reason)
		}
		candidates = safe
	} else {
		candidates = append(append([]preemption.Candidate(nil), cycle.ordinary...), candidates...)
	}
	candidate := pl.evaluator.SelectCandidate(ctx, pod, candidates)
	if candidate == nil {
		return framework.NewPostFilterResultWithNominatedNode(""), framework.NewStatus(framework.Unschedulable, preemptionNoCandidateMessage)
	}
	if pl.profile == decisionLogProfile {
		klog.FromContext(ctx).Info("Selected placement preemption candidate", "profile", pl.profile, "pod", klog.KObj(pod), "decisionID", pod.UID,
			"attempt", framework.SchedulingDecisionAttemptFromState(state), "placement_phase", placementPhase(preference),
			"node", candidate.Name(), "pdb_violations", candidate.Victims().NumPDBViolations)
	}
	return pl.evaluator.ExecuteCandidate(ctx, pod, candidate)
}

func placementPhase(preference *framework.NodeResourcePreference) string {
	if preference.AllowFallback {
		return "fallback"
	}
	return "preferred"
}

func (pl *NamespaceResourceGuarantee) allowRDMA(ctx context.Context, state *framework.CycleState, pod *v1.Pod, reason string) (*framework.PostFilterResult, *framework.Status) {
	framework.NodeResourcePreferenceFromState(state).AllowFallback = true
	if pl.profile == decisionLogProfile {
		klog.FromContext(ctx).Info("Allowing RDMA placement fallback", "profile", pl.profile, "pod", klog.KObj(pod), "decisionID", pod.UID,
			"attempt", framework.SchedulingDecisionAttemptFromState(state), "placement_phase", "fallback", "fallback_reason", reason)
	}
	return &framework.PostFilterResult{RetryScheduling: true}, nil
}

// nominatedPlacement recognizes only a placement that fits now or will fit once
// its already-preempted victims terminate. A changed hard constraint or a missing
// node invalidates the nomination instead of keeping the pod waiting indefinitely.
func (pl *NamespaceResourceGuarantee) nominatedPlacement(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (bool, bool, error) {
	preference := framework.NodeResourcePreferenceFromState(state)
	if preference.NodeNames != nil && !preference.NodeNames.Has(pod.Status.NominatedNodeName) {
		return false, false, nil
	}
	nodes, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return false, false, err
	}
	var node *framework.NodeInfo
	for _, candidate := range nodes {
		if candidate.Node().Name == pod.Status.NominatedNodeName {
			node = candidate.Snapshot()
			break
		}
	}
	if node == nil {
		return false, false, nil
	}
	probe := state.Clone()
	probe.IsPreemptionDryRun = true
	framework.NodeResourcePreferenceFromState(probe).AllowFallback = true
	fits := func() (bool, error) {
		status := pl.handle.RunFilterPluginsWithNominatedPods(ctx, probe, pod, node)
		if status.Code() == framework.Error {
			return false, status.AsError()
		}
		if !status.IsSuccess() {
			return false, nil
		}
		nodes := []*framework.NodeInfo{node}
		for _, extender := range pl.handle.Extenders() {
			if !extender.IsInterested(pod) {
				continue
			}
			filtered, _, _, err := extender.Filter(pod, nodes)
			if err != nil {
				if extender.IsIgnorable() {
					continue
				}
				return false, err
			}
			if len(filtered) == 0 {
				return false, nil
			}
			nodes = filtered
		}
		return true, nil
	}
	if ready, err := fits(); ready || err != nil {
		return ready, false, err
	}
	removed := false
	for _, victim := range append([]*framework.PodInfo(nil), node.Pods...) {
		if corev1helpers.PodPriority(victim.Pod) >= corev1helpers.PodPriority(pod) || !podTerminatingByPreemption(victim.Pod) {
			continue
		}
		if err := node.RemovePod(klog.FromContext(ctx), victim.Pod); err != nil {
			return false, false, err
		}
		if status := pl.handle.RunPreFilterExtensionRemovePod(ctx, probe, pod, victim, node); !status.IsSuccess() {
			return false, false, status.AsError()
		}
		removed = true
	}
	if !removed {
		return false, false, nil
	}
	waiting, err := fits()
	return false, waiting, err
}
