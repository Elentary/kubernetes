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

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.NominatedNodeReservation

	// ErrReasonNodeReserved is the scheduling reason reported when a node is reserved.
	ErrReasonNodeReserved = "node reserved for namespace resource guarantee preemption"

	// Event reasons emitted on the holder pod as reservation state changes.
	EventReasonNodeReserved             = "NamespaceResourceGuaranteeNodeReserved"
	EventReasonNodeReservationReleased  = "NamespaceResourceGuaranteeNodeReservationReleased"
	EventReasonNodeReservationCleanedUp = "NamespaceResourceGuaranteeNodeReservationCleanedUp"
)

// NominatedNodeReservation blocks scheduling on nodes reserved for quota preemption.
type NominatedNodeReservation struct {
	handle    framework.Handle
	profile   string
	podLister corelisters.PodLister
	store     *Store
}

var _ framework.FilterPlugin = &NominatedNodeReservation{}
var _ framework.PostBindPlugin = &NominatedNodeReservation{}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	RegisterMetrics()
	pl := &NominatedNodeReservation{handle: handle, profile: profileName(handle), store: SharedStore()}
	if handle != nil && handle.SharedInformerFactory() != nil {
		pl.podLister = handle.SharedInformerFactory().Core().V1().Pods().Lister()
	}
	return pl, nil
}

// Name returns the plugin name.
func (pl *NominatedNodeReservation) Name() string {
	return Name
}

// Filter blocks nodes reserved for quota preemption from all non-holder pods.
func (pl *NominatedNodeReservation) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node info is missing node object")
	}

	reservation, ok := pl.store.Get(node.Name)
	if !ok {
		return nil
	}
	if reservation.HolderPodUID == pod.UID {
		return nil
	}

	logger := klog.FromContext(ctx)
	if pl.podLister != nil {
		holderPod, err := pl.podLister.Pods(reservation.HolderNamespace).Get(reservation.HolderName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				if state != nil && state.IsPreemptionDryRun {
					return nil
				}
				pl.cleanupStaleReservation(ctx, node.Name, reservation, nil, transitionReasonHolderNotFound, "holder pod not found")
				return nil
			}
			logger.Error(err, "Failed to read holder pod for nominated node reservation", "node", node.Name, "holder", klog.KRef(reservation.HolderNamespace, reservation.HolderName), "holderUID", reservation.HolderPodUID)
		} else if stale, reason, detail := isStaleReservation(holderPod, reservation, node.Name); stale {
			if state != nil && state.IsPreemptionDryRun {
				return nil
			}
			pl.cleanupStaleReservation(ctx, node.Name, reservation, holderPod, reason, detail)
			return nil
		}
	}

	recordBlockedPod(pl.profile)
	logger.V(3).Info("Blocking pod on reserved nominated node", "pod", klog.KObj(pod), "node", node.Name, "holder", klog.KRef(reservation.HolderNamespace, reservation.HolderName), "holderUID", reservation.HolderPodUID)
	reason := fmt.Sprintf("%s by %s/%s", ErrReasonNodeReserved, reservation.HolderNamespace, reservation.HolderName)
	return framework.NewStatus(framework.Unschedulable, reason)
}

// PostBind releases reservation once the holder pod is bound.
func (pl *NominatedNodeReservation) PostBind(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) {
	released, ok := pl.store.ReleaseByPod(pod.UID)
	if !ok {
		return
	}
	RecordTransition(transitionActionReleased, transitionReasonHolderBound)

	logger := klog.FromContext(ctx)
	logger.V(2).Info("Released nominated node reservation after holder pod bound", "pod", klog.KObj(pod), "node", released.NodeName, "holderUID", released.HolderPodUID)
	if pl.handle != nil && pl.handle.EventRecorder() != nil {
		pl.handle.EventRecorder().Eventf(
			pod,
			nil,
			v1.EventTypeNormal,
			EventReasonNodeReservationReleased,
			Name,
			"Released nominated-node reservation for node %s: holder pod bound",
			released.NodeName,
		)
	}
}

func (pl *NominatedNodeReservation) cleanupStaleReservation(ctx context.Context, nodeName string, reservation Reservation, holderPod *v1.Pod, reason, detail string) {
	released, ok := pl.store.ReleaseIfMatches(nodeName, reservation.HolderPodUID)
	if !ok {
		return
	}
	RecordTransition(transitionActionCleaned, reason)

	logger := klog.FromContext(ctx)
	logger.V(2).Info("Cleaned up stale nominated node reservation", "node", nodeName, "holder", klog.KRef(reservation.HolderNamespace, reservation.HolderName), "holderUID", reservation.HolderPodUID, "reason", detail)

	if holderPod != nil && pl.handle != nil && pl.handle.EventRecorder() != nil {
		pl.handle.EventRecorder().Eventf(
			holderPod,
			nil,
			v1.EventTypeNormal,
			EventReasonNodeReservationCleanedUp,
			Name,
			"Cleaned stale nominated-node reservation for node %s: %s",
			released.NodeName,
			detail,
		)
	}
}

func isStaleReservation(holderPod *v1.Pod, reservation Reservation, nodeName string) (bool, string, string) {
	if holderPod.UID != reservation.HolderPodUID {
		return true, transitionReasonHolderUIDChanged, "holder UID changed"
	}
	if holderPod.DeletionTimestamp != nil {
		return true, transitionReasonHolderTerminating, "holder pod is terminating"
	}
	if holderPod.Spec.NodeName != "" {
		return true, transitionReasonHolderAlreadyBound, fmt.Sprintf("holder pod already bound to %s", holderPod.Spec.NodeName)
	}
	if holderPod.Status.NominatedNodeName != "" && holderPod.Status.NominatedNodeName != nodeName {
		return true, transitionReasonNominationMoved, fmt.Sprintf("holder nominatedNodeName moved to %s", holderPod.Status.NominatedNodeName)
	}
	return false, "", ""
}

func profileName(handle framework.Handle) string {
	if handle == nil {
		return ""
	}
	if profileAwareHandle, ok := handle.(interface{ ProfileName() string }); ok {
		return profileAwareHandle.ProfileName()
	}
	return ""
}
