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
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
)

func TestRDMAPodClassification(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1.Pod, *config.NamespaceResourceGuaranteeArgs)
		want   bool
	}{
		{name: "guaranteed GPU", want: true},
		{name: "disabled", mutate: func(_ *v1.Pod, a *config.NamespaceResourceGuaranteeArgs) {
			a.PreferNonRDMANodesForGuaranteedGPU = false
		}},
		{name: "ordinary tier", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) { p.Spec.PriorityClassName = "ordinary" }},
		{name: "semi tier", mutate: func(p *v1.Pod, a *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.PriorityClassName = "semi"
			a.SemiProtectedPriorityClassName = "semi"
		}},
		{name: "CPU only", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}
		}},
		{name: "MIG only", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.Containers[0].Resources.Requests = v1.ResourceList{"nvidia.com/mig-1g.10gb": resource.MustParse("1")}
		}},
		{name: "RDMA request", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.Containers[0].Resources.Requests[rdmaResource] = resource.MustParse("1")
		}},
		{name: "zero RDMA request", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.Containers[0].Resources.Requests[rdmaResource] = resource.MustParse("0")
		}, want: true},
		{name: "GPU init container", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.InitContainers = p.Spec.Containers
			p.Spec.Containers = []v1.Container{{Name: "main"}}
		}, want: true},
		{name: "RDMA init container", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			p.Spec.InitContainers = []v1.Container{{Name: "init", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{rdmaResource: resource.MustParse("1")}}}}
		}},
		{name: "RDMA restartable init container", mutate: func(p *v1.Pod, _ *config.NamespaceResourceGuaranteeArgs) {
			always := v1.ContainerRestartPolicyAlways
			p.Spec.InitContainers = []v1.Container{{Name: "sidecar", RestartPolicy: &always, Resources: v1.ResourceRequirements{Requests: v1.ResourceList{rdmaResource: resource.MustParse("1")}}}}
		}},
		{name: "GPU does not need a configured guarantee", mutate: func(_ *v1.Pod, a *config.NamespaceResourceGuaranteeArgs) {
			a.NamespaceGuarantees = map[string]v1.ResourceList{"team-a": {v1.ResourceCPU: resource.MustParse("1")}}
		}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := newArgs(map[string]v1.ResourceList{"team-a": {protectedGPUResource: resource.MustParse("8")}})
			args.PreferNonRDMANodesForGuaranteedGPU = true
			pod := makePodWithRequests("incoming", "team-a", "protected", "", map[v1.ResourceName]string{protectedGPUResource: "1"})
			if tt.mutate != nil {
				tt.mutate(pod, &args)
			}
			pl := &NamespaceResourceGuarantee{args: args}
			if got := pl.prefersNonRDMA(pod); got != tt.want {
				t.Errorf("classification=%v want=%v", got, tt.want)
			}
		})
	}
}

func TestRDMAProfileValidation(t *testing.T) {
	metrics.Register()
	for _, tt := range []struct {
		name                        string
		extensions                  []string
		defaultPreemption, disabled bool
		wantError                   string
	}{
		{name: "valid", extensions: []string{"PreFilter", "Filter", "PostFilter"}},
		{name: "missing prefilter", extensions: []string{"Filter", "PostFilter"}, wantError: "preFilter"},
		{name: "missing filter", extensions: []string{"PreFilter", "PostFilter"}, wantError: "filter"},
		{name: "missing postfilter", extensions: []string{"PreFilter", "Filter"}, wantError: "postFilter"},
		{name: "competing default preemption", extensions: []string{"PreFilter", "Filter", "PostFilter"}, defaultPreemption: true, wantError: "DefaultPreemption"},
		{name: "disabled backstop allowed", extensions: []string{"PreFilter"}, disabled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := newArgs(map[string]v1.ResourceList{"team-a": {protectedGPUResource: resource.MustParse("8")}})
			args.PreferNonRDMANodesForGuaranteedGPU = !tt.disabled
			register := []tf.RegisterPluginFunc{
				tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New), tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
				tf.RegisterPluginAsExtensions(Name, func(ctx context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
					return New(ctx, &args, handle)
				}, tt.extensions...),
			}
			if tt.defaultPreemption {
				register = append(register, tf.RegisterPluginAsExtensions(defaultpreemption.Name, frameworkruntime.FactoryAdapter(feature.Features{}, defaultpreemption.New), "PostFilter"))
			}
			cs := clientsetfake.NewClientset()
			fwk, err := tf.NewFramework(context.Background(), register, "better-scheduler", frameworkruntime.WithClientSet(cs), frameworkruntime.WithInformerFactory(informers.NewSharedInformerFactory(cs, 0)))
			if err == nil {
				defer fwk.Close()
			}
			if tt.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error=%v want %q", err, tt.wantError)
			}
		})
	}
}

func TestRDMAMultiPointValidation(t *testing.T) {
	metrics.Register()
	for _, disableFilter := range []bool{false, true} {
		args := newArgs(map[string]v1.ResourceList{"team-a": {protectedGPUResource: resource.MustParse("8")}})
		args.PreferNonRDMANodesForGuaranteedGPU = true
		register := []tf.RegisterPluginFunc{
			tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New), tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
			tf.RegisterPluginAsExtensions(Name, func(ctx context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
				return New(ctx, &args, handle)
			}),
			func(_ *frameworkruntime.Registry, profile *config.KubeSchedulerProfile) {
				profile.Plugins.MultiPoint.Enabled = []config.Plugin{{Name: Name, Weight: 1}}
				if disableFilter {
					profile.Plugins.Filter.Disabled = []config.Plugin{{Name: Name}}
				}
			},
		}
		cs := clientsetfake.NewClientset()
		fwk, err := tf.NewFramework(context.Background(), register, "better-scheduler", frameworkruntime.WithClientSet(cs), frameworkruntime.WithInformerFactory(informers.NewSharedInformerFactory(cs, 0)))
		if err == nil {
			fwk.Close()
		}
		if disableFilter {
			if err == nil || !strings.Contains(err.Error(), "filter") {
				t.Fatalf("expected missing filter error, got %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
