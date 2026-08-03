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
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Reservation tracks a reserved nominated node.
type Reservation struct {
	NodeName        string
	HolderPodUID    types.UID
	HolderNamespace string
	HolderName      string
	Source          string
	ReservedAt      time.Time
}

// Store is a process-local reservation store shared by plugin instances across profiles.
type Store struct {
	mu     sync.RWMutex
	byNode map[string]Reservation
	byPod  map[types.UID]string
}

var sharedStore = newStore()

func newStore() *Store {
	return &Store{
		byNode: make(map[string]Reservation),
		byPod:  make(map[types.UID]string),
	}
}

// SharedStore returns the process-wide reservation store.
func SharedStore() *Store {
	return sharedStore
}

// ResetSharedStoreForTest clears all reservations from the shared store.
func ResetSharedStoreForTest() {
	sharedStore.Reset()
}

// Reserve upserts a node reservation for the given pod.
// It returns the resulting reservation and whether it changed store state.
func (s *Store) Reserve(nodeName string, pod *v1.Pod, source string) (Reservation, bool) {
	if pod == nil || nodeName == "" {
		return Reservation{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existingNode, ok := s.byPod[pod.UID]; ok && existingNode != nodeName {
		delete(s.byNode, existingNode)
	}

	if existing, ok := s.byNode[nodeName]; ok {
		if existing.HolderPodUID == pod.UID && existing.HolderNamespace == pod.Namespace && existing.HolderName == pod.Name && existing.Source == source {
			s.byPod[pod.UID] = nodeName
			return existing, false
		}
		delete(s.byPod, existing.HolderPodUID)
	}

	reservation := Reservation{
		NodeName:        nodeName,
		HolderPodUID:    pod.UID,
		HolderNamespace: pod.Namespace,
		HolderName:      pod.Name,
		Source:          source,
		ReservedAt:      time.Now(),
	}
	s.byNode[nodeName] = reservation
	s.byPod[pod.UID] = nodeName
	nominatedNodeReservationsActive.Set(float64(len(s.byNode)))

	return reservation, true
}

// ReleaseByPod releases reservation held by the given pod UID.
func (s *Store) ReleaseByPod(uid types.UID) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nodeName, ok := s.byPod[uid]
	if !ok {
		return Reservation{}, false
	}
	reservation, ok := s.byNode[nodeName]
	if !ok || reservation.HolderPodUID != uid {
		delete(s.byPod, uid)
		return Reservation{}, false
	}

	delete(s.byPod, uid)
	delete(s.byNode, nodeName)
	nominatedNodeReservationsActive.Set(float64(len(s.byNode)))
	return reservation, true
}

// ReleaseIfMatches releases reservation for nodeName only when uid matches the holder.
func (s *Store) ReleaseIfMatches(nodeName string, uid types.UID) (Reservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	reservation, ok := s.byNode[nodeName]
	if !ok || reservation.HolderPodUID != uid {
		return Reservation{}, false
	}

	delete(s.byNode, nodeName)
	delete(s.byPod, uid)
	nominatedNodeReservationsActive.Set(float64(len(s.byNode)))
	return reservation, true
}

// Get returns reservation for a node.
func (s *Store) Get(nodeName string) (Reservation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, ok := s.byNode[nodeName]
	return reservation, ok
}

// Reset clears store state.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byNode = make(map[string]Reservation)
	s.byPod = make(map[types.UID]string)
	nominatedNodeReservationsActive.Set(0)
}
