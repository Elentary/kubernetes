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
	"math/rand"
	"sort"
	"strings"
	"sync"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	policylisters "k8s.io/client-go/listers/policy/v1"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	configvalidation "k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nominatednodereservation"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	schedmetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.NamespaceResourceGuarantee

	defaultMinCandidateNodesPercentage int32 = 10
	defaultMinCandidateNodesAbsolute   int32 = 100
	protectedGPUResource                     = v1.ResourceName("nvidia.com/gpu")
	decisionLogProfile                       = "better-scheduler"

	preemptionWaitingOnTerminatingVictims = "not eligible due to a terminating pod on the nominated node."
	preemptionNoCandidateMessage          = "no candidate node for preemption"
	preemptionNotHelpfulFragment          = "Preemption is not helpful for scheduling"

	preemptionStartedReason     = "NamespaceResourceGuaranteePreemptionStarted"
	preemptionWaitingReason     = "NamespaceResourceGuaranteePreemptionWaiting"
	preemptionNotHelpfulReason  = "NamespaceResourceGuaranteePreemptionNotHelpful"
	preemptionNoCandidateReason = "NamespaceResourceGuaranteePreemptionNoCandidate"
	preemptionErrorReason       = "NamespaceResourceGuaranteePreemptionError"
)

// NamespaceResourceGuarantee enforces per-namespace protected resource guarantees.
type NamespaceResourceGuarantee struct {
	handle             framework.Handle
	profile            string
	args               config.NamespaceResourceGuaranteeArgs
	configuredResource []v1.ResourceName
	pdbLister          policylisters.PodDisruptionBudgetLister
	evaluator          *preemption.Evaluator
	preemptionTrace    sync.Map // map[types.UID]*preemptionDecisionTrace
}

type protectedTier string

const (
	guaranteedTier protectedTier = "guaranteed"
	semiTier       protectedTier = "semi-guaranteed"
)

var _ framework.PreFilterPlugin = &NamespaceResourceGuarantee{}
var _ framework.EnqueueExtensions = &NamespaceResourceGuarantee{}
var _ framework.PostFilterPlugin = &NamespaceResourceGuarantee{}
var _ framework.PreEnqueuePlugin = &NamespaceResourceGuarantee{}
var _ framework.ScorePlugin = &NamespaceResourceGuarantee{}
var _ preemption.Interface = &NamespaceResourceGuarantee{}

type resourceDeficit struct {
	resourceName v1.ResourceName
	deficit      int64
	request      int64
}

type nodePreemptionTrace struct {
	nodeName              string
	deficientResources    []resourceDeficit
	numPDBViolatingVictim int
	victims               []*v1.Pod
	statusCode            framework.Code
	statusMessage         string
	packingScore          *preemptionCandidatePackingScore
}

type preemptionCandidatePackingScore struct {
	score                       int64
	incomingGPU                 int64
	allocatableGPU              int64
	protectedGPUBefore          int64
	protectedGPUAfterVictims    int64
	protectedGPUAfterScheduling int64
}

type preemptionDecisionTrace struct {
	mu    sync.Mutex
	nodes map[string]*nodePreemptionTrace
}

type preemptionEvent struct {
	eventType     string
	reason        string
	nominatedNode string
	note          string
}

// Name returns the plugin name.
func (pl *NamespaceResourceGuarantee) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(_ context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.NamespaceResourceGuaranteeArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *NamespaceResourceGuaranteeArgs", obj)
	}
	if err := configvalidation.ValidateNamespaceResourceGuaranteeArgs(nil, args); err != nil {
		return nil, err
	}

	plugin := &NamespaceResourceGuarantee{
		handle:             handle,
		profile:            profileName(handle),
		args:               *args,
		configuredResource: configuredResources(args.NamespaceGuarantees),
		pdbLister:          getPDBLister(handle),
	}
	registerMetrics()
	nominatednodereservation.RegisterMetrics()
	plugin.recordConfiguredQuotaMetrics(plugin.profile)
	if handle != nil && handle.SharedInformerFactory() != nil {
		plugin.evaluator = preemption.NewEvaluator(Name, handle, plugin, utilfeature.DefaultFeatureGate.Enabled(features.SchedulerAsyncPreemption))

		originalPreemptPod := plugin.evaluator.PreemptPod
		plugin.evaluator.PreemptPod = func(ctx context.Context, c preemption.Candidate, preemptor, victim *v1.Pod, pluginName string) error {
			if err := originalPreemptPod(ctx, c, preemptor, victim, pluginName); err != nil {
				return err
			}
			if preemptor != nil {
				tier := plugin.podTier(preemptor)
				namespaceResourceGuaranteeVictimDeletions.WithLabelValues(
					plugin.profile,
					preemptor.Namespace,
					string(tier),
				).Inc()
			}
			plugin.handle.EventRecorder().Eventf(
				victim,
				preemptor,
				v1.EventTypeNormal,
				"NamespaceResourceGuaranteePreempted",
				"NamespaceResourceGuaranteePreempting",
				"Preempted by %s/%s on node %s (decisionID=%s)",
				preemptor.Namespace,
				preemptor.Name,
				c.Name(),
				preemptor.UID,
			)
			return nil
		}
	}

	return plugin, nil
}

// preFilterQuota checks whether a protected pod would exceed its namespace's shared protected resource guarantee.
func (pl *NamespaceResourceGuarantee) preFilterQuota(_ context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	tier := pl.podTier(pod)
	if tier == "" || len(pl.configuredResource) == 0 {
		return nil, nil
	}

	requested := pl.protectedPodRequests(pod)
	if len(requested) == 0 {
		return nil, nil
	}

	currentUsage, err := pl.namespaceProtectedUsage(pod.Namespace)
	if err != nil {
		pl.recordPreFilterDecision(pod, tier, preFilterResultError)
		return nil, framework.AsStatus(err)
	}

	_, namespaceConfigured := pl.args.NamespaceGuarantees[pod.Namespace]
	for _, resourceName := range pl.configuredResource {
		// A configured namespace is capped only for resources it explicitly lists.
		// Keep the zero-guarantee backstop for namespaces absent from the map.
		if namespaceConfigured {
			if _, resourceConfigured := pl.args.NamespaceGuarantees[pod.Namespace][resourceName]; !resourceConfigured {
				continue
			}
		}
		resourceRequested := requested[resourceName]
		if resourceRequested == 0 {
			continue
		}
		resourceUsage := currentUsage[resourceName]
		resourceGuarantee := pl.namespaceGuaranteeValue(pod.Namespace, resourceName)
		if resourceUsage+resourceRequested > resourceGuarantee {
			pl.recordPreFilterDecision(pod, tier, preFilterResultQuotaExceeded)
			pl.recordQuotaExceeded(pod, tier, resourceName)
			return nil, framework.NewStatus(
				framework.UnschedulableAndUnresolvable,
				fmt.Sprintf(
					"namespace %q shared protected resource guarantee exceeded: resource=%q guarantee=%d current=%d requested=%d",
					pod.Namespace,
					resourceName,
					resourceGuarantee,
					resourceUsage,
					resourceRequested,
				),
			)
		}
	}

	pl.recordPreFilterDecision(pod, tier, preFilterResultAllowed)
	return nil, nil
}

// PreFilterExtensions returns nil because this plugin maintains no incremental state.
func (pl *NamespaceResourceGuarantee) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// EventsToRegister returns the pod events that can reduce protected namespace resource usage.
func (pl *NamespaceResourceGuarantee) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{
		{
			Event:          framework.ClusterEvent{Resource: framework.Pod, ActionType: framework.Delete | framework.UpdatePodScaleDown},
			QueueingHintFn: pl.isSchedulableAfterPodChange,
		},
	}, nil
}

func (pl *NamespaceResourceGuarantee) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, m framework.NodeToStatusReader) (*framework.PostFilterResult, *framework.Status) {
	if !pl.isProtectedPod(pod) || len(pl.configuredResource) == 0 {
		pl.recordPreemptionOutcome(pod, "ordinary", preemptionOutcomeIneligible)
		return nil, framework.NewStatus(framework.Unschedulable, "namespace resource guarantee preemption is only enabled for protected pods")
	}
	tier := string(pl.podTier(pod))
	if pl.evaluator == nil {
		pl.recordPreemptionOutcome(pod, tier, preemptionOutcomeError)
		return nil, framework.NewStatus(framework.Error, "namespace resource guarantee preemption evaluator is not initialized")
	}

	trace, err := pl.newPreemptionTrace(pod)
	if err != nil {
		pl.recordPreemptionOutcome(pod, tier, preemptionOutcomeError)
		return nil, framework.AsStatus(err)
	}
	if cycle := rdmaStateFromCycle(state); cycle != nil {
		if cycle.trace != nil {
			trace = cycle.trace
		} else {
			cycle.trace = trace
		}
	}
	pl.preemptionTrace.Store(pod.UID, trace)
	defer pl.preemptionTrace.Delete(pod.UID)

	var result *framework.PostFilterResult
	var status *framework.Status
	if framework.NodeResourcePreferenceFromState(state) != nil {
		result, status = pl.postFilterRDMA(ctx, state, pod, m)
	} else {
		result, status = pl.evaluator.Preempt(ctx, state, pod, m)
	}
	if result != nil && result.RetryScheduling {
		return result, status
	}
	schedmetrics.PreemptionAttempts.Inc()
	pl.logPreemptionDecision(ctx, state, pod, result, status)
	pl.syncNominatedNodeReservation(ctx, pod, result, status)

	msg := status.Message()
	if len(msg) > 0 {
		return result, framework.NewStatus(status.Code(), "preemption: "+msg)
	}
	return result, status
}

// Score favors nodes that already have protected GPU usage, which helps pack
// protected GPU pods onto fewer nodes and reduces fragmentation.
func (pl *NamespaceResourceGuarantee) Score(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	incomingGPU := pl.scoredProtectedGPURequest(pod)
	if incomingGPU == 0 {
		return 0, nil
	}

	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	allocatableGPU := nodeAllocatableForResource(nodeInfo, protectedGPUResource)
	if allocatableGPU <= 0 {
		return 0, nil
	}

	protectedGPUUsage := pl.nodeProtectedResourceUsage(nodeInfo, protectedGPUResource)
	return protectedGPUPackingScore(incomingGPU, allocatableGPU, protectedGPUUsage), nil
}

// ScoreExtensions returns nil because the score is already normalized.
func (pl *NamespaceResourceGuarantee) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func (pl *NamespaceResourceGuarantee) syncNominatedNodeReservation(ctx context.Context, pod *v1.Pod, result *framework.PostFilterResult, status *framework.Status) {
	logger := klog.FromContext(ctx)
	store := nominatednodereservation.SharedStore()

	if status != nil && status.IsSuccess() && result != nil && len(result.NominatedNodeName) > 0 {
		reservation, changed := store.Reserve(result.NominatedNodeName, pod, Name)
		if changed {
			nominatednodereservation.RecordTransition("reserved", "preemption_started")
			logger.V(2).Info("Reserved nominated node for namespace resource guarantee preemption", "preemptor", klog.KObj(pod), "node", reservation.NodeName, "holderUID", reservation.HolderPodUID)
			pl.handle.EventRecorder().Eventf(
				pod,
				nil,
				v1.EventTypeNormal,
				nominatednodereservation.EventReasonNodeReserved,
				"NamespaceResourceGuaranteePostFilter",
				"Reserved nominated node %s for quota preemption",
				reservation.NodeName,
			)
		}
		return
	}

	if result != nil && result.Mode() == framework.ModeOverride && len(result.NominatedNodeName) == 0 {
		if released, ok := store.ReleaseByPod(pod.UID); ok {
			nominatednodereservation.RecordTransition("released", "nomination_cleared")
			logger.V(2).Info("Released nominated node reservation after preemption cleared nomination", "preemptor", klog.KObj(pod), "node", released.NodeName, "holderUID", released.HolderPodUID)
			pl.handle.EventRecorder().Eventf(
				pod,
				nil,
				v1.EventTypeNormal,
				nominatednodereservation.EventReasonNodeReservationReleased,
				"NamespaceResourceGuaranteePostFilter",
				"Released nominated-node reservation for node %s: nomination cleared",
				released.NodeName,
			)
		}
		return
	}

	if status == nil || status.Code() != framework.Unschedulable {
		return
	}
	msg := status.Message()
	if msg == preemptionWaitingOnTerminatingVictims {
		return
	}
	if msg == preemptionNoCandidateMessage || strings.Contains(msg, preemptionNotHelpfulFragment) {
		if released, ok := store.ReleaseByPod(pod.UID); ok {
			nominatednodereservation.RecordTransition("released", "no_candidate")
			logger.V(2).Info("Released nominated node reservation after preemption found no candidate", "preemptor", klog.KObj(pod), "node", released.NodeName, "holderUID", released.HolderPodUID, "message", msg)
			pl.handle.EventRecorder().Eventf(
				pod,
				nil,
				v1.EventTypeNormal,
				nominatednodereservation.EventReasonNodeReservationReleased,
				"NamespaceResourceGuaranteePostFilter",
				"Released nominated-node reservation for node %s: %s",
				released.NodeName,
				msg,
			)
		}
	}
}

func (pl *NamespaceResourceGuarantee) PreEnqueue(_ context.Context, pod *v1.Pod) *framework.Status {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SchedulerAsyncPreemption) {
		return nil
	}
	if pl.evaluator == nil {
		return nil
	}
	if pl.evaluator.IsPodRunningPreemption(pod.GetUID()) {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable, "waiting for the preemption for this pod to be finished")
	}
	return nil
}

func (pl *NamespaceResourceGuarantee) GetOffsetAndNumCandidates(numNodes int32) (int32, int32) {
	return rand.Int31n(numNodes), pl.calculateNumCandidates(numNodes)
}

func (pl *NamespaceResourceGuarantee) CandidatesToVictimsMap(candidates []preemption.Candidate) map[string]*extenderv1.Victims {
	m := make(map[string]*extenderv1.Victims, len(candidates))
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

func (pl *NamespaceResourceGuarantee) PodEligibleToPreemptOthers(_ context.Context, pod *v1.Pod, nominatedNodeStatus *framework.Status) (bool, string) {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}

	nodeInfos := pl.handle.SnapshotSharedLister().NodeInfos()
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		if nominatedNodeStatus.Code() == framework.UnschedulableAndUnresolvable {
			return true, ""
		}
		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			podPriority := corev1helpers.PodPriority(pod)
			for _, p := range nodeInfo.Pods {
				if corev1helpers.PodPriority(p.Pod) < podPriority && podTerminatingByPreemption(p.Pod) {
					return false, preemptionWaitingOnTerminatingVictims
				}
			}
		}
	}
	return true, ""
}

func (pl *NamespaceResourceGuarantee) OrderedScoreFuncs(ctx context.Context, pod *v1.Pod, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64 {
	scores := pl.packingScoreFuncs(ctx, pod, nodesToVictims)
	if !pl.prefersNonRDMA(pod) {
		return scores
	}
	preferOrdinary := func(name string) int64 {
		node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(name)
		if err == nil && !framework.NodeHasResource(node.Node(), rdmaResource) {
			return 1
		}
		return 0
	}
	return append([]func(string) int64{preferOrdinary}, scores...)
}

func (pl *NamespaceResourceGuarantee) packingScoreFuncs(_ context.Context, pod *v1.Pod, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64 {
	if pl.scoredProtectedGPURequest(pod) == 0 {
		return nil
	}

	scores := make(map[string]preemptionCandidatePackingScore, len(nodesToVictims))
	for nodeName, victims := range nodesToVictims {
		score, ok := pl.preemptionProtectedGPUPackingScore(pod, nodeName, victims)
		if !ok {
			continue
		}
		scores[nodeName] = score
		pl.recordNodePreemptionScore(pod.UID, nodeName, score)
	}
	if len(scores) == 0 {
		return nil
	}

	return []func(node string) int64{
		func(node string) int64 {
			if score, ok := scores[node]; ok {
				return score.score
			}
			return 0
		},
	}
}

func (pl *NamespaceResourceGuarantee) SelectVictimsOnNode(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeInfo *framework.NodeInfo,
	pdbs []*policy.PodDisruptionBudget,
) ([]*v1.Pod, int, *framework.Status) {
	logger := klog.FromContext(ctx)
	var potentialVictims []*framework.PodInfo
	removePod := func(rpi *framework.PodInfo) error {
		if err := nodeInfo.RemovePod(logger, rpi.Pod); err != nil {
			return err
		}
		status := pl.handle.RunPreFilterExtensionRemovePod(ctx, state, pod, rpi, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}
	addPod := func(api *framework.PodInfo) error {
		nodeInfo.AddPodInfo(api)
		status := pl.handle.RunPreFilterExtensionAddPod(ctx, state, pod, api, nodeInfo)
		if !status.IsSuccess() {
			return status.AsError()
		}
		return nil
	}

	deficientResources := pl.orderedDeficientResources(nodeInfo, pod)

	podPriority := corev1helpers.PodPriority(pod)
	for _, pi := range nodeInfo.Pods {
		if corev1helpers.PodPriority(pi.Pod) < podPriority && pl.isEligiblePreemptionVictim(pod, pi.Pod) {
			potentialVictims = append(potentialVictims, pi)
			if err := removePod(pi); err != nil {
				return nil, 0, framework.AsStatus(err)
			}
		}
	}

	if len(potentialVictims) == 0 {
		status := framework.NewStatus(framework.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
		pl.recordNodeTrace(pod.UID, &nodePreemptionTrace{
			nodeName:           nodeInfo.Node().Name,
			deficientResources: deficientResources,
			statusCode:         status.Code(),
			statusMessage:      status.Message(),
		})
		return nil, 0, status
	}

	if status := pl.handle.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo); !status.IsSuccess() {
		pl.recordNodeTrace(pod.UID, &nodePreemptionTrace{
			nodeName:           nodeInfo.Node().Name,
			deficientResources: deficientResources,
			statusCode:         status.Code(),
			statusMessage:      status.Message(),
		})
		return nil, 0, status
	}

	sort.Slice(potentialVictims, func(i, j int) bool {
		return schedutil.MoreImportantPod(potentialVictims[i].Pod, potentialVictims[j].Pod)
	})

	var victims []*v1.Pod
	numViolatingVictim := 0
	violatingVictims, nonViolatingVictims := filterPodsWithPDBViolation(potentialVictims, pdbs)
	reprievePod := func(pi *framework.PodInfo) (bool, error) {
		if err := addPod(pi); err != nil {
			return false, err
		}
		status := pl.handle.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if status.Code() == framework.Error {
			return false, status.AsError()
		}
		fits := status.IsSuccess()
		if !fits {
			if err := removePod(pi); err != nil {
				return false, err
			}
			victims = append(victims, pi.Pod)
		}
		return fits, nil
	}
	for _, p := range violatingVictims {
		if fits, err := reprievePod(p); err != nil {
			return nil, 0, framework.AsStatus(err)
		} else if !fits {
			numViolatingVictim++
		}
	}
	for _, p := range nonViolatingVictims {
		if _, err := reprievePod(p); err != nil {
			return nil, 0, framework.AsStatus(err)
		}
	}

	if len(violatingVictims) != 0 && len(nonViolatingVictims) != 0 {
		sort.Slice(victims, func(i, j int) bool { return schedutil.MoreImportantPod(victims[i], victims[j]) })
	}

	pl.recordNodeTrace(pod.UID, &nodePreemptionTrace{
		nodeName:              nodeInfo.Node().Name,
		deficientResources:    deficientResources,
		numPDBViolatingVictim: numViolatingVictim,
		victims:               append([]*v1.Pod(nil), victims...),
		statusCode:            framework.Success,
	})
	return victims, numViolatingVictim, framework.NewStatus(framework.Success)
}

func (pl *NamespaceResourceGuarantee) isEligiblePreemptionVictim(preemptor, victim *v1.Pod) bool {
	if !pl.args.RestrictGuaranteedPreemptionToManagedNamespaces || pl.podTier(preemptor) != guaranteedTier {
		return true
	}
	_, managed := pl.args.NamespaceGuarantees[victim.Namespace]
	return managed
}

func (pl *NamespaceResourceGuarantee) isSchedulableAfterPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (framework.QueueingHint, error) {
	originalPod, modifiedPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return framework.Queue, err
	}

	// The unschedulable pod itself may become schedulable when it scales down.
	if modifiedPod != nil && modifiedPod.UID == pod.UID {
		if pl.requestDecreased(originalPod, modifiedPod) {
			logger.V(5).Info("protected pod scaled down and may now fit under the namespace resource guarantee", "pod", klog.KObj(pod))
			return framework.Queue, nil
		}
		logger.V(5).Info("protected pod update did not reduce its relevant resource request", "pod", klog.KObj(pod))
		return framework.QueueSkip, nil
	}

	if pl.namespaceUsageDecreased(originalPod, modifiedPod, pod.Namespace) {
		logger.V(5).Info("namespace protected resource usage decreased and may unblock scheduling", "pod", klog.KObj(pod))
		return framework.Queue, nil
	}

	logger.V(5).Info("pod change did not reduce relevant namespace protected resource usage", "pod", klog.KObj(pod))
	return framework.QueueSkip, nil
}

func (pl *NamespaceResourceGuarantee) namespaceProtectedUsage(namespace string) (map[v1.ResourceName]int64, error) {
	usage := make(map[v1.ResourceName]int64, len(pl.configuredResource))
	for _, resourceName := range pl.configuredResource {
		usage[resourceName] = 0
	}

	sharedLister := pl.handle.SnapshotSharedLister()
	if sharedLister == nil {
		return nil, fmt.Errorf("snapshot shared lister is not available")
	}

	nodeInfos, err := sharedLister.NodeInfos().List()
	if err != nil {
		return nil, err
	}

	for _, nodeInfo := range nodeInfos {
		if nodeInfo == nil {
			continue
		}

		for _, podInfo := range nodeInfo.Pods {
			if podInfo.Pod == nil || podInfo.Pod.Spec.NodeName == "" || podInfo.Pod.Namespace != namespace || !pl.isProtectedPod(podInfo.Pod) {
				continue
			}
			requests := pl.podRequests(podInfo.Pod)
			for _, resourceName := range pl.configuredResource {
				usage[resourceName] += quantityValue(requests[resourceName], resourceName)
			}
		}
	}

	return usage, nil
}

func (pl *NamespaceResourceGuarantee) scheduledTierResourceRequest(pod *v1.Pod, namespace string, tier protectedTier, resourceName v1.ResourceName) int64 {
	if pod == nil || pod.Spec.NodeName == "" || pod.Namespace != namespace {
		return 0
	}
	if pl.podTier(pod) != tier {
		return 0
	}
	return pl.resourceRequest(pod, resourceName)
}

func (pl *NamespaceResourceGuarantee) protectedResourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {
	if !pl.isProtectedPod(pod) {
		return 0
	}
	return pl.resourceRequest(pod, resourceName)
}

func (pl *NamespaceResourceGuarantee) isProtectedPod(pod *v1.Pod) bool {
	return pl.podTier(pod) != ""
}

func (pl *NamespaceResourceGuarantee) podTier(pod *v1.Pod) protectedTier {
	if pod == nil {
		return ""
	}
	if pod.Spec.PriorityClassName == pl.args.ProtectedPriorityClassName {
		return guaranteedTier
	}
	if len(pl.args.SemiProtectedPriorityClassName) > 0 && pod.Spec.PriorityClassName == pl.args.SemiProtectedPriorityClassName {
		return semiTier
	}
	return ""
}

func (pl *NamespaceResourceGuarantee) protectedPodRequests(pod *v1.Pod) map[v1.ResourceName]int64 {
	requested := make(map[v1.ResourceName]int64, len(pl.configuredResource))
	for _, resourceName := range pl.configuredResource {
		value := pl.protectedResourceRequest(pod, resourceName)
		if value > 0 {
			requested[resourceName] = value
		}
	}
	return requested
}

func protectedGPUPackingScore(incomingGPU, allocatableGPU, protectedGPUUsage int64) int64 {
	if allocatableGPU <= 0 || incomingGPU <= 0 {
		return 0
	}
	if protectedGPUUsage < 0 {
		protectedGPUUsage = 0
	}
	if protectedGPUUsage > allocatableGPU {
		protectedGPUUsage = allocatableGPU
	}
	requestedAfterScheduling := protectedGPUUsage + incomingGPU
	if requestedAfterScheduling > allocatableGPU {
		requestedAfterScheduling = allocatableGPU
	}
	return (requestedAfterScheduling * framework.MaxNodeScore) / allocatableGPU
}

func (pl *NamespaceResourceGuarantee) preemptionProtectedGPUPackingScore(pod *v1.Pod, nodeName string, victims *extenderv1.Victims) (preemptionCandidatePackingScore, bool) {
	incomingGPU := pl.scoredProtectedGPURequest(pod)
	if incomingGPU == 0 {
		return preemptionCandidatePackingScore{}, false
	}
	nodeInfo, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return preemptionCandidatePackingScore{}, false
	}
	allocatableGPU := nodeAllocatableForResource(nodeInfo, protectedGPUResource)
	if allocatableGPU <= 0 {
		return preemptionCandidatePackingScore{}, false
	}

	protectedGPUBefore := pl.nodeProtectedResourceUsage(nodeInfo, protectedGPUResource)
	protectedGPUAfterVictims := protectedGPUBefore
	if victims != nil {
		for _, victim := range victims.Pods {
			protectedGPUAfterVictims -= pl.protectedResourceRequest(victim, protectedGPUResource)
		}
	}
	if protectedGPUAfterVictims < 0 {
		protectedGPUAfterVictims = 0
	}
	if protectedGPUAfterVictims > allocatableGPU {
		protectedGPUAfterVictims = allocatableGPU
	}

	protectedGPUAfterScheduling := protectedGPUAfterVictims + incomingGPU
	if protectedGPUAfterScheduling > allocatableGPU {
		protectedGPUAfterScheduling = allocatableGPU
	}
	return preemptionCandidatePackingScore{
		score:                       protectedGPUPackingScore(incomingGPU, allocatableGPU, protectedGPUAfterVictims),
		incomingGPU:                 incomingGPU,
		allocatableGPU:              allocatableGPU,
		protectedGPUBefore:          protectedGPUBefore,
		protectedGPUAfterVictims:    protectedGPUAfterVictims,
		protectedGPUAfterScheduling: protectedGPUAfterScheduling,
	}, true
}

func (pl *NamespaceResourceGuarantee) scoredProtectedGPURequest(pod *v1.Pod) int64 {
	if !pl.hasConfiguredResource(protectedGPUResource) {
		return 0
	}
	return pl.protectedResourceRequest(pod, protectedGPUResource)
}

func (pl *NamespaceResourceGuarantee) hasConfiguredResource(resourceName v1.ResourceName) bool {
	for _, configured := range pl.configuredResource {
		if configured == resourceName {
			return true
		}
	}
	return false
}

func (pl *NamespaceResourceGuarantee) requestDecreased(originalPod, modifiedPod *v1.Pod) bool {
	for _, resourceName := range pl.configuredResource {
		if pl.protectedResourceRequest(modifiedPod, resourceName) < pl.protectedResourceRequest(originalPod, resourceName) {
			return true
		}
	}
	return false
}

func (pl *NamespaceResourceGuarantee) calculateNumCandidates(numNodes int32) int32 {
	n := (numNodes * defaultMinCandidateNodesPercentage) / 100
	if n < defaultMinCandidateNodesAbsolute {
		n = defaultMinCandidateNodesAbsolute
	}
	if n > numNodes {
		n = numNodes
	}
	return n
}

func (pl *NamespaceResourceGuarantee) orderedDeficientResources(nodeInfo *framework.NodeInfo, pod *v1.Pod) []resourceDeficit {
	var deficits []resourceDeficit
	for _, resourceName := range pl.configuredResource {
		request := pl.resourceRequest(pod, resourceName)
		if request == 0 {
			continue
		}
		free := nodeAllocatableForResource(nodeInfo, resourceName) - nodeRequestedForResource(nodeInfo, resourceName)
		deficit := request - free
		if deficit > 0 {
			deficits = append(deficits, resourceDeficit{
				resourceName: resourceName,
				deficit:      deficit,
				request:      request,
			})
		}
	}
	sort.Slice(deficits, func(i, j int) bool {
		left := deficits[i]
		right := deficits[j]
		// Compare normalized severity deficit/request without floating point.
		lhs := left.deficit * right.request
		rhs := right.deficit * left.request
		if lhs != rhs {
			return lhs > rhs
		}
		return left.resourceName < right.resourceName
	})
	return deficits
}

func (pl *NamespaceResourceGuarantee) newPreemptionTrace(_ *v1.Pod) (*preemptionDecisionTrace, error) {
	return &preemptionDecisionTrace{
		nodes: make(map[string]*nodePreemptionTrace),
	}, nil
}

func (pl *NamespaceResourceGuarantee) recordNodeTrace(podUID types.UID, trace *nodePreemptionTrace) {
	d, ok := pl.lookupTrace(podUID)
	if !ok {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes[trace.nodeName] = trace
}

func (pl *NamespaceResourceGuarantee) recordNodePreemptionScore(podUID types.UID, nodeName string, score preemptionCandidatePackingScore) {
	d, ok := pl.lookupTrace(podUID)
	if !ok {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	nodeTrace, ok := d.nodes[nodeName]
	if !ok {
		nodeTrace = &nodePreemptionTrace{nodeName: nodeName, statusCode: framework.Success}
		d.nodes[nodeName] = nodeTrace
	}
	nodeTrace.packingScore = &score
}

func (pl *NamespaceResourceGuarantee) lookupTrace(podUID types.UID) (*preemptionDecisionTrace, bool) {
	v, ok := pl.preemptionTrace.Load(podUID)
	if !ok {
		return nil, false
	}
	trace, ok := v.(*preemptionDecisionTrace)
	if !ok {
		return nil, false
	}
	return trace, true
}

func (pl *NamespaceResourceGuarantee) logPreemptionDecision(
	ctx context.Context,
	state *framework.CycleState,
	preemptor *v1.Pod,
	result *framework.PostFilterResult,
	status *framework.Status,
) {
	logger := klog.FromContext(ctx)
	trace, ok := pl.lookupTrace(preemptor.UID)
	if !ok {
		return
	}

	trace.mu.Lock()
	defer trace.mu.Unlock()

	event := classifyPreemptionEvent(preemptor, result, status, trace)
	tier := pl.podTier(preemptor)
	pl.recordPreemptionOutcome(preemptor, string(tier), preemptionOutcomeForEventReason(event.reason))
	pl.handle.EventRecorder().Eventf(preemptor, nil, event.eventType, event.reason, "NamespaceResourceGuaranteePostFilter", event.note)

	keyvals := preemptionDecisionLogKeyvals(preemptor, state)
	for nodeName, nodeTrace := range trace.nodes {
		selected := nodeName == event.nominatedNode && nodeTrace.statusCode == framework.Success
		reason := "candidate not selected"
		if nodeTrace.statusCode != framework.Success {
			reason = nodeTrace.statusMessage
		} else if selected {
			reason = "selected"
		}
		if nodeTrace.packingScore != nil {
			logger.Info(
				"Preemption candidate scored for pod",
				append(
					preemptionCandidateScoreKeyvals(keyvals, nodeName, nodeTrace),
					"selected", selected,
				)...,
			)
		}
		if selected {
			logger.Info(
				"Preemption candidate selected for pod",
				append(
					preemptionCandidateScoreKeyvals(keyvals, nodeName, nodeTrace),
					"selection_path", "preemption",
				)...,
			)
		}
		logger.V(2).Info(
			"NamespaceResourceGuarantee preemption decision",
			"decisionID", preemptor.UID,
			"attempt", framework.SchedulingDecisionAttemptFromState(state),
			"preemptor", klog.KObj(preemptor),
			"node", nodeName,
			"statusCode", nodeTrace.statusCode,
			"deficientResources", formatDeficits(nodeTrace.deficientResources),
			"victims", podKeys(nodeTrace.victims),
			"numPDBViolatingVictims", nodeTrace.numPDBViolatingVictim,
			"score", preemptionCandidateScore(nodeTrace),
			"selected", selected,
			"reason", reason,
		)
	}
}

func preemptionDecisionLogKeyvals(preemptor *v1.Pod, state *framework.CycleState) []interface{} {
	return []interface{}{
		"profile", decisionLogProfile,
		"decisionID", string(preemptor.UID),
		"attempt", framework.SchedulingDecisionAttemptFromState(state),
		"pod", klog.KObj(preemptor),
	}
}

func preemptionCandidateScoreKeyvals(base []interface{}, nodeName string, nodeTrace *nodePreemptionTrace) []interface{} {
	keyvals := append([]interface{}{}, base...)
	keyvals = append(keyvals,
		"node", nodeName,
		"score", preemptionCandidateScore(nodeTrace),
		"score_plugin", Name,
		"score_name", "ProtectedGPUPacking",
		"victims", podKeys(nodeTrace.victims),
		"numPDBViolatingVictims", nodeTrace.numPDBViolatingVictim,
	)
	if nodeTrace.packingScore != nil {
		score := nodeTrace.packingScore
		keyvals = append(keyvals,
			"incoming_gpu", score.incomingGPU,
			"allocatable_gpu", score.allocatableGPU,
			"protected_gpu_before", score.protectedGPUBefore,
			"protected_gpu_after_victims", score.protectedGPUAfterVictims,
			"protected_gpu_after_scheduling", score.protectedGPUAfterScheduling,
		)
	}
	return keyvals
}

func preemptionCandidateScore(nodeTrace *nodePreemptionTrace) int64 {
	if nodeTrace == nil || nodeTrace.packingScore == nil {
		return 0
	}
	return nodeTrace.packingScore.score
}

func classifyPreemptionEvent(preemptor *v1.Pod, result *framework.PostFilterResult, status *framework.Status, trace *preemptionDecisionTrace) preemptionEvent {
	nominatedNode := preemptor.Status.NominatedNodeName
	if result != nil && len(result.NominatedNodeName) > 0 {
		nominatedNode = result.NominatedNodeName
	}

	if status == nil {
		return preemptionEvent{
			eventType:     v1.EventTypeWarning,
			reason:        preemptionErrorReason,
			nominatedNode: nominatedNode,
			note:          fmt.Sprintf("decisionID=%s phase=error reason=%q", preemptor.UID, "missing preemption status"),
		}
	}

	switch status.Code() {
	case framework.Success:
		victims := selectedVictimKeys(trace, nominatedNode)
		return preemptionEvent{
			eventType:     v1.EventTypeNormal,
			reason:        preemptionStartedReason,
			nominatedNode: nominatedNode,
			note:          formatStartedPreemptionEventNote(preemptor.UID, nominatedNode, victims),
		}
	case framework.Error:
		return preemptionEvent{
			eventType:     v1.EventTypeWarning,
			reason:        preemptionErrorReason,
			nominatedNode: nominatedNode,
			note:          truncatePreemptionEventNote(fmt.Sprintf("decisionID=%s phase=error reason=%q", preemptor.UID, status.Message())),
		}
	default:
		if status.Message() == preemptionWaitingOnTerminatingVictims && len(nominatedNode) > 0 {
			return preemptionEvent{
				eventType:     v1.EventTypeNormal,
				reason:        preemptionWaitingReason,
				nominatedNode: nominatedNode,
				note: truncatePreemptionEventNote(fmt.Sprintf(
					"decisionID=%s phase=waiting nominatedNode=%s reason=%q",
					preemptor.UID,
					nominatedNode,
					"waiting for preempted pods to terminate",
				)),
			}
		}
		if strings.Contains(status.Message(), preemptionNotHelpfulFragment) {
			return preemptionEvent{
				eventType:     v1.EventTypeNormal,
				reason:        preemptionNotHelpfulReason,
				nominatedNode: nominatedNode,
				note:          truncatePreemptionEventNote(fmt.Sprintf("decisionID=%s phase=not-helpful reason=%q", preemptor.UID, status.Message())),
			}
		}
		if status.Message() == preemptionNoCandidateMessage || len(nominatedNode) == 0 {
			return preemptionEvent{
				eventType:     v1.EventTypeNormal,
				reason:        preemptionNoCandidateReason,
				nominatedNode: nominatedNode,
				note:          truncatePreemptionEventNote(fmt.Sprintf("decisionID=%s phase=no-candidate reason=%q", preemptor.UID, status.Message())),
			}
		}

		return preemptionEvent{
			eventType:     v1.EventTypeNormal,
			reason:        preemptionNoCandidateReason,
			nominatedNode: nominatedNode,
			note:          truncatePreemptionEventNote(fmt.Sprintf("decisionID=%s phase=no-candidate nominatedNode=%s reason=%q", preemptor.UID, nominatedNode, status.Message())),
		}
	}
}

func formatStartedPreemptionEventNote(preemptorUID types.UID, nominatedNode string, victims []string) string {
	const startedNote = "preemption initiated; victim termination may still be in progress"

	prefix := fmt.Sprintf("decisionID=%s phase=started nominatedNode=%s victims=[", preemptorUID, nominatedNode)
	suffix := fmt.Sprintf("] note=%q", startedNote)
	availableVictimChars := corevalidation.NoteLengthLimit - len(prefix) - len(suffix)

	return truncatePreemptionEventNote(prefix + summarizeVictimKeysForEvent(victims, availableVictimChars) + suffix)
}

func summarizeVictimKeysForEvent(victims []string, maxLen int) string {
	if len(victims) == 0 || maxLen <= 0 {
		return ""
	}

	joined := strings.Join(victims, ",")
	if len(joined) <= maxLen {
		return joined
	}

	var b strings.Builder
	maxOmittedSuffixLen := len(fmt.Sprintf(",...(+%d more)", len(victims)))
	for i, victim := range victims {
		separatorLen := 0
		if b.Len() > 0 {
			separatorLen = 1
		}

		if b.Len()+separatorLen+len(victim)+maxOmittedSuffixLen > maxLen {
			if b.Len() == 0 {
				summary := fmt.Sprintf("%d victims", len(victims))
				if len(summary) > maxLen {
					return summary[:maxLen]
				}
				return summary
			}
			b.WriteString(fmt.Sprintf(",...(+%d more)", len(victims)-i))
			return b.String()
		}

		if separatorLen == 1 {
			b.WriteByte(',')
		}
		b.WriteString(victim)
	}

	return b.String()
}

func truncatePreemptionEventNote(note string) string {
	if len(note) <= corevalidation.NoteLengthLimit {
		return note
	}
	suffix := " ..."
	return note[:corevalidation.NoteLengthLimit-len(suffix)] + suffix
}

func selectedVictimKeys(trace *preemptionDecisionTrace, nominatedNode string) []string {
	if trace == nil || len(nominatedNode) == 0 {
		return nil
	}
	if nodeTrace, ok := trace.nodes[nominatedNode]; ok {
		return podKeys(nodeTrace.victims)
	}
	return nil
}

func podTerminatingByPreemption(p *v1.Pod) bool {
	if p.DeletionTimestamp == nil {
		return false
	}
	for _, condition := range p.Status.Conditions {
		if condition.Type == v1.DisruptionTarget {
			return condition.Status == v1.ConditionTrue && condition.Reason == v1.PodReasonPreemptionByScheduler
		}
	}
	return false
}

func filterPodsWithPDBViolation(podInfos []*framework.PodInfo, pdbs []*policy.PodDisruptionBudget) (violatingPodInfos, nonViolatingPodInfos []*framework.PodInfo) {
	pdbsAllowed := make([]int32, len(pdbs))
	for i, pdb := range pdbs {
		pdbsAllowed[i] = pdb.Status.DisruptionsAllowed
	}
	for _, podInfo := range podInfos {
		pod := podInfo.Pod
		pdbForPodIsViolated := false
		if len(pod.Labels) != 0 {
			for i, pdb := range pdbs {
				if pdb.Namespace != pod.Namespace {
					continue
				}
				selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
				if err != nil {
					continue
				}
				if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
					continue
				}
				if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
					continue
				}
				pdbsAllowed[i]--
				if pdbsAllowed[i] < 0 {
					pdbForPodIsViolated = true
				}
			}
		}
		if pdbForPodIsViolated {
			violatingPodInfos = append(violatingPodInfos, podInfo)
		} else {
			nonViolatingPodInfos = append(nonViolatingPodInfos, podInfo)
		}
	}
	return violatingPodInfos, nonViolatingPodInfos
}

func nodeRequestedForResource(nodeInfo *framework.NodeInfo, resourceName v1.ResourceName) int64 {
	switch resourceName {
	case v1.ResourceCPU:
		return nodeInfo.Requested.MilliCPU
	case v1.ResourceMemory:
		return nodeInfo.Requested.Memory
	default:
		return nodeInfo.Requested.ScalarResources[resourceName]
	}
}

func nodeAllocatableForResource(nodeInfo *framework.NodeInfo, resourceName v1.ResourceName) int64 {
	switch resourceName {
	case v1.ResourceCPU:
		return nodeInfo.Allocatable.MilliCPU
	case v1.ResourceMemory:
		return nodeInfo.Allocatable.Memory
	default:
		return nodeInfo.Allocatable.ScalarResources[resourceName]
	}
}

func (pl *NamespaceResourceGuarantee) nodeProtectedResourceUsage(nodeInfo *framework.NodeInfo, resourceName v1.ResourceName) int64 {
	if nodeInfo == nil {
		return 0
	}

	var usage int64
	for _, podInfo := range nodeInfo.Pods {
		usage += pl.protectedResourceRequest(podInfo.Pod, resourceName)
	}
	return usage
}

func formatDeficits(deficits []resourceDeficit) []string {
	out := make([]string, 0, len(deficits))
	for _, deficit := range deficits {
		out = append(out, fmt.Sprintf("%s:%d/%d", deficit.resourceName, deficit.deficit, deficit.request))
	}
	return out
}

func podKeys(pods []*v1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, pod := range pods {
		out = append(out, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
	}
	sort.Strings(out)
	return out
}

func getPDBLister(handle framework.Handle) policylisters.PodDisruptionBudgetLister {
	if handle == nil || handle.SharedInformerFactory() == nil {
		return nil
	}
	return handle.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister()
}

func (pl *NamespaceResourceGuarantee) namespaceUsageDecreased(originalPod, modifiedPod *v1.Pod, namespace string) bool {
	tier := pl.podTier(originalPod)
	if tier == "" {
		return false
	}
	for _, resourceName := range pl.configuredResource {
		if pl.scheduledTierResourceRequest(originalPod, namespace, tier, resourceName) > pl.scheduledTierResourceRequest(modifiedPod, namespace, tier, resourceName) {
			return true
		}
	}
	return false
}

func (pl *NamespaceResourceGuarantee) namespaceGuaranteeValue(namespace string, resourceName v1.ResourceName) int64 {
	guaranteeByNamespace := pl.args.NamespaceGuarantees[namespace]
	return quantityValue(guaranteeByNamespace[resourceName], resourceName)
}

func (pl *NamespaceResourceGuarantee) resourceRequest(pod *v1.Pod, resourceName v1.ResourceName) int64 {
	return quantityValue(pl.podRequests(pod)[resourceName], resourceName)
}

func (pl *NamespaceResourceGuarantee) podRequests(pod *v1.Pod) v1.ResourceList {
	if pod == nil {
		return nil
	}

	return resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{
		UseStatusResources:    utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		SkipPodLevelResources: !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
	})
}

func quantityValue(quantity resource.Quantity, resourceName v1.ResourceName) int64 {
	switch resourceName {
	case v1.ResourceCPU:
		return quantity.MilliValue()
	default:
		return quantity.Value()
	}
}

func configuredResources(namespaceGuarantees map[string]v1.ResourceList) []v1.ResourceName {
	resourceSet := map[v1.ResourceName]struct{}{}
	for _, namespaceResources := range namespaceGuarantees {
		for resourceName := range namespaceResources {
			resourceSet[resourceName] = struct{}{}
		}
	}

	resources := make([]v1.ResourceName, 0, len(resourceSet))
	for resourceName := range resourceSet {
		resources = append(resources, resourceName)
	}
	sort.Slice(resources, func(i, j int) bool {
		return resources[i] < resources[j]
	})
	return resources
}
