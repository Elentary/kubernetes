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

package betterimage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/kubernetes/test/integration/framework"
)

func TestMain(m *testing.M) { framework.EtcdMain(m.Run) }

// Run explicitly with BETTER_SCHEDULER_TEST_IMAGE. The Docker daemon must share
// the host network and filesystem with the test runner. No live cluster is used.
func TestReleasedImage(t *testing.T) {
	image := os.Getenv("BETTER_SCHEDULER_TEST_IMAGE")
	if image == "" {
		t.Skip("set BETTER_SCHEDULER_TEST_IMAGE to test a built image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, rest, stop := framework.StartTestServer(ctx, t, framework.TestServerSetup{})
	defer stop()
	dir := t.TempDir()
	ca, err := os.ReadFile(rest.CAFile)
	if err != nil {
		t.Fatal(err)
	}
	cfg := clientcmdapi.Config{
		Clusters:  map[string]*clientcmdapi.Cluster{"test": {Server: rest.Host, CertificateAuthorityData: ca}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {Token: rest.BearerToken}},
		Contexts:  map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}}, CurrentContext: "test",
	}
	if err := clientcmd.WriteToFile(cfg, filepath.Join(dir, "kubeconfig")); err != nil {
		t.Fatal(err)
	}
	config := `apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: /test/kubeconfig
leaderElection:
  leaderElect: false
profiles:
- schedulerName: better-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: NamespaceResourceGuarantee
      - name: NominatedNodeReservation
      disabled:
      - name: DefaultPreemption
  pluginConfig:
  - name: NamespaceResourceGuarantee
    args:
      protectedPriorityClassName: guaranteed
      namespaceGuarantees:
        image-smoke:
          cpu: "1"
`
	if err := os.WriteFile(filepath.Join(dir, "scheduler.yaml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("better-scheduler-smoke-%d", time.Now().UnixNano())
	out, err := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name, "--network=host", "-v", dir+":/test:ro", image, "--config=/test/scheduler.yaml", "--secure-port=0").CombinedOutput()
	if err != nil {
		t.Fatalf("start image: %s: %v", out, err)
	}
	defer func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Log(string(logs))
		}
		if out, err := exec.Command("docker", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Errorf("remove image test container: %s: %v", out, err)
		}
	}()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "image-smoke"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: "guaranteed"}, Value: 1000}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "smoke-node"}, Status: v1.NodeStatus{Capacity: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4"), v1.ResourceMemory: resource.MustParse("8Gi"), v1.ResourcePods: resource.MustParse("32")}, Allocatable: v1.ResourceList{v1.ResourceCPU: resource.MustParse("4"), v1.ResourceMemory: resource.MustParse("8Gi"), v1.ResourcePods: resource.MustParse("32")}}}
	if _, err := client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"first", "blocked"} {
		pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "image-smoke"}, Spec: v1.PodSpec{SchedulerName: "better-scheduler", PriorityClassName: "guaranteed", Containers: []v1.Container{{Name: "test", Image: "registry.k8s.io/pause:3.10", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}}}}}}
		if _, err := client.CoreV1().Pods("image-smoke").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			p, err := client.CoreV1().Pods("image-smoke").Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if name == "first" {
				return p.Spec.NodeName == "smoke-node", nil
			}
			if p.Spec.NodeName != "" {
				return false, fmt.Errorf("quota-blocked pod was bound")
			}
			for _, c := range p.Status.Conditions {
				if c.Type == v1.PodScheduled && strings.Contains(c.Message, "guarantee exceeded") {
					return true, nil
				}
			}
			return false, nil
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	zero := int64(0)
	if err := client.CoreV1().Pods("image-smoke").Delete(ctx, "first", metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := client.CoreV1().Pods("image-smoke").Get(ctx, "blocked", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return p.Spec.NodeName == "smoke-node", nil
	}); err != nil {
		t.Fatalf("quota release did not requeue and bind pod: %v", err)
	}
}
