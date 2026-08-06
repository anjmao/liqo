// Copyright 2019-2026 The Liqo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package virtualnodectrl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	k8strings "k8s.io/utils/strings"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	networkingv1beta1 "github.com/liqotech/liqo/apis/networking/v1beta1"
	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	foreigncluster "github.com/liqotech/liqo/pkg/utils/foreigncluster"
	"github.com/liqotech/liqo/pkg/utils/getters"
	"github.com/liqotech/liqo/pkg/utils/resource"
	"github.com/liqotech/liqo/pkg/vkMachinery"
	vkforge "github.com/liqotech/liqo/pkg/vkMachinery/forge"
	vkutils "github.com/liqotech/liqo/pkg/vkMachinery/utils"
)

const offloadingPatchHashAnnotation = "liqo.io/offloading-patch-hash"

// createVirtualKubeletDeployment creates the VirtualKubelet Deployment.
func (r *VirtualNodeReconciler) ensureVirtualKubeletDeploymentPresence(
	ctx context.Context, virtualNode *offloadingv1beta1.VirtualNode) (err error) {
	var nodeStatusInitial offloadingv1beta1.VirtualNodeConditionStatusType
	if *virtualNode.Spec.CreateNode {
		nodeStatusInitial = offloadingv1beta1.CreatingConditionStatusType
	} else {
		nodeStatusInitial = offloadingv1beta1.NoneConditionStatusType
	}
	defer func() {
		if interr := r.Client.Status().Update(ctx, virtualNode); interr != nil {
			if err != nil {
				klog.Error(err)
			}
			err = fmt.Errorf("error updating virtual node status: %w", interr)
		}
	}()

	ForgeCondition(virtualNode,
		VnConditionMap{
			offloadingv1beta1.VirtualKubeletConditionType: VnCondition{
				Status: offloadingv1beta1.CreatingConditionStatusType,
			},
			offloadingv1beta1.NodeConditionType: VnCondition{Status: nodeStatusInitial},
		},
	)

	namespace := virtualNode.Namespace
	name := virtualNode.Name
	remoteClusterID := virtualNode.Spec.ClusterID
	// create the base resources
	vkServiceAccount := vkforge.VirtualKubeletServiceAccount(namespace, name)
	var op controllerutil.OperationResult
	op, err = resource.CreateOrUpdate(ctx, r.Client, vkServiceAccount, func() error {
		return nil
	})
	if err != nil {
		return err
	}
	klog.V(5).Infof("[%v] ServiceAccount %s/%s reconciled: %s",
		remoteClusterID, vkServiceAccount.Namespace, vkServiceAccount.Name, op)

	vkClusterRoleBinding := vkforge.VirtualKubeletClusterRoleBinding(namespace, name, remoteClusterID)
	op, err = resource.CreateOrUpdate(ctx, r.Client, vkClusterRoleBinding, func() error {
		return nil
	})
	if err != nil {
		return err
	}

	klog.V(5).Infof("[%v] ClusterRoleBinding %s reconciled: %s",
		remoteClusterID, vkClusterRoleBinding.Name, op)

	// Fetch the VkOptionsTemplate referenced by the VirtualNode (or the default one).
	vkOpts, err := r.getVkOptionsTemplate(ctx, virtualNode)
	if err != nil {
		return fmt.Errorf("unable to fetch the VkOptionsTemplate for virtual-node %q: %w", virtualNode.Name, err)
	}

	// Forge the virtual Kubelet Deployment directly from the VirtualNode and the VkOptionsTemplate.
	vkDeployment := vkforge.VirtualKubeletDeployment(r.HomeClusterID, r.LiqoNamespace, r.LocalPodCIDRs, virtualNode, vkOpts)

	// Retrieve the ForeignCluster to check whether networking is enabled for the remote cluster.
	fc, err := foreigncluster.GetForeignClusterByID(ctx, r.Client, virtualNode.Spec.ClusterID)
	if err != nil {
		return fmt.Errorf("unable to retrieve the ForeignCluster %q: %w", virtualNode.Spec.ClusterID, err)
	}

	var cfg *networkingv1beta1.Configuration
	if fc.Status.Modules.Networking.Enabled {
		// Networking module is enabled: retrieve the network configuration.
		cfg, err = getters.GetConfigurationByClusterID(ctx, r.Client, virtualNode.Spec.ClusterID, corev1.NamespaceAll)
		if err != nil {
			return fmt.Errorf("unable to retrieve the network configuration for the ForeignCluster %q: %w", virtualNode.Spec.ClusterID, err)
		}
	}

	// CreateOrUpdate the Deployment using the forged deployment as the desired state.
	op, err = resource.CreateOrUpdate(ctx, r.Client, vkDeployment, func() error {
		if cfg != nil && len(vkDeployment.Spec.Template.Spec.Containers) > 0 {
			vkDeployment.Spec.Template.Spec.Containers[0].Args =
				vkforge.SetNetworkConfigurationArgs(vkDeployment.Spec.Template.Spec.Containers[0].Args, cfg)
		}

		// Add the hash of the offloading patch as annotation.
		opHash, err := offloadingPatchHash(virtualNode.Spec.OffloadingPatch)
		if err != nil {
			return err
		}
		if vkDeployment.Spec.Template.Annotations == nil {
			vkDeployment.Spec.Template.Annotations = make(map[string]string)
		}
		vkDeployment.Spec.Template.Annotations[offloadingPatchHashAnnotation] = opHash

		return nil
	})
	if err != nil {
		return err
	}
	klog.V(5).Infof("[%v] Deployment %s/%s reconciled: %s",
		remoteClusterID, vkDeployment.Namespace, vkDeployment.Name, op)

	if op == controllerutil.OperationResultCreated {
		msg := fmt.Sprintf("[%v] Launching virtual-kubelet %s in namespace %v",
			remoteClusterID, vkDeployment.Name, namespace)
		klog.Info(msg)
		r.EventsRecorder.Event(virtualNode, "Normal", "VkCreated", msg)
	}

	ForgeCondition(virtualNode,
		VnConditionMap{
			offloadingv1beta1.VirtualKubeletConditionType: VnCondition{
				Status: offloadingv1beta1.RunningConditionStatusType,
			},
		})

	if *virtualNode.Spec.CreateNode {
		ForgeCondition(virtualNode,
			VnConditionMap{
				offloadingv1beta1.NodeConditionType: VnCondition{
					Status: offloadingv1beta1.RunningConditionStatusType,
				},
			})
	}
	return err
}

// ensureVirtualKubeletDeploymentAbsence deletes the VirtualKubelet Deployment.
// It checks if the VirtualKubelet Pods have been deleted.
func (r *VirtualNodeReconciler) ensureVirtualKubeletDeploymentAbsence(
	ctx context.Context, virtualNode *offloadingv1beta1.VirtualNode) error {
	virtualKubeletDeployment, err := vkutils.GetVirtualKubeletDeployment(ctx, r.Client, virtualNode)
	if err != nil {
		return err
	}
	if virtualKubeletDeployment != nil {
		msg := fmt.Sprintf("[%v] Deleting virtual-kubelet in namespace %v", virtualNode.Spec.ClusterID, virtualNode.Namespace)
		klog.Info(msg)
		r.EventsRecorder.Event(virtualNode, "Normal", "VkDeleted", msg)

		if err := r.Client.Delete(ctx, virtualKubeletDeployment); err != nil {
			return err
		}
	}

	if err := vkutils.CheckVirtualKubeletPodAbsence(ctx, r.Client, virtualNode); err != nil {
		return err
	}

	crbName := k8strings.ShortenString(fmt.Sprintf("%s%s", vkMachinery.CRBPrefix, virtualNode.Name), 253)
	err = r.Client.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{
		Name: crbName,
	}})
	if client.IgnoreNotFound(err) != nil {
		return err
	}
	klog.Info(fmt.Sprintf("[%v] Deleted virtual-kubelet CRB %s", virtualNode.Spec.ClusterID, crbName))

	err = r.Client.Delete(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: virtualNode.Name, Namespace: virtualNode.Namespace,
	}})
	if client.IgnoreNotFound(err) != nil {
		return err
	}

	return nil
}

// getVkOptionsTemplate fetches the VkOptionsTemplate referenced by the VirtualNode, or the default
// one configured on the reconciler if the VirtualNode does not specify a reference. It also defaults
// the VirtualNode CreateNode and DisableNetworkCheck fields from the template (mirroring the
// removed webhook behavior) so the VN spec is self-describing.
func (r *VirtualNodeReconciler) getVkOptionsTemplate(ctx context.Context,
	virtualNode *offloadingv1beta1.VirtualNode) (*offloadingv1beta1.VkOptionsTemplate, error) {
	ref := r.VkOptionsDefaultTemplate
	if virtualNode.Spec.VkOptionsTemplateRef != nil {
		ref = virtualNode.Spec.VkOptionsTemplateRef
	}
	if ref == nil {
		return nil, fmt.Errorf("no VkOptionsTemplate reference configured for virtual-node %q and no default set", virtualNode.Name)
	}

	var vkOpts offloadingv1beta1.VkOptionsTemplate
	if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &vkOpts); err != nil {
		return nil, err
	}

	// Default CreateNode and DisableNetworkCheck from the template if not set on the VirtualNode.
	// This mirrors the defaulting previously performed by the mutating webhook.
	if virtualNode.Spec.CreateNode == nil {
		createNode := vkOpts.Spec.CreateNode
		virtualNode.Spec.CreateNode = &createNode
	}
	if virtualNode.Spec.DisableNetworkCheck == nil {
		disableNetworkCheck := vkOpts.Spec.DisableNetworkCheck
		virtualNode.Spec.DisableNetworkCheck = &disableNetworkCheck
	}

	return &vkOpts, nil
}

func offloadingPatchHash(offloadingPatch *offloadingv1beta1.OffloadingPatch) (string, error) {
	if offloadingPatch == nil {
		return "", nil
	}

	opString, err := json.Marshal(offloadingPatch)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	opHash := sha256.Sum256(opString)
	opHashHex := hex.EncodeToString(opHash[:])

	return opHashHex, nil
}
