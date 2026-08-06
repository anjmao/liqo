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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	authv1beta1 "github.com/liqotech/liqo/apis/authentication/v1beta1"
	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	"github.com/liqotech/liqo/pkg/consts"
	liqoctlrauthentication "github.com/liqotech/liqo/pkg/liqo-controller-manager/authentication"
	"github.com/liqotech/liqo/pkg/liqo-controller-manager/offloading/forge"
	"github.com/liqotech/liqo/pkg/utils/getters"
	"github.com/liqotech/liqo/pkg/utils/resource"
)

// reconcileResourceSlice creates or updates the VirtualNode associated with a ResourceSlice.
// This logic was moved from the standalone virtualnodecreator-controller.
func (r *VirtualNodeReconciler) reconcileResourceSlice(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	var resourceSlice authv1beta1.ResourceSlice
	if err := r.Get(ctx, req.NamespacedName, &resourceSlice); err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("resourceSlice %q not found", req.NamespacedName)
			return reconcile.Result{}, nil
		}
		klog.Errorf("unable to get ResourceSlice %q: %v", req.NamespacedName, err)
		return reconcile.Result{}, err
	}

	if resourceSlice.DeletionTimestamp != nil {
		klog.V(6).Infof("ResourceSlice %q is being deleted", req.NamespacedName)
		return reconcile.Result{}, nil
	}

	if resourceSlice.Annotations == nil ||
		resourceSlice.Annotations[consts.CreateVirtualNodeAnnotation] == "" ||
		strings.EqualFold(resourceSlice.Annotations[consts.CreateVirtualNodeAnnotation], "false") {
		klog.V(6).Infof("VirtualNode creation disabled for resourceslice %q", req.NamespacedName)
		return reconcile.Result{}, nil
	}

	if !allConditionsAccepted(&resourceSlice) {
		klog.V(6).Infof("Not all ResourceSlice %q conditions are yet accepted", req.NamespacedName)
		return reconcile.Result{}, nil
	}

	if resourceSlice.Labels == nil || resourceSlice.Labels[consts.RemoteClusterID] == "" {
		err := fmt.Errorf("resourceslice %q does not contain the remote cluster ID label", req.NamespacedName)
		klog.Error(err)
		return reconcile.Result{}, err
	}

	remoteClusterID := liqov1beta1.ClusterID(resourceSlice.Labels[consts.RemoteClusterID])

	// Get the associated Identity for the remote cluster.
	identity, err := getters.GetIdentityFromResourceSlice(ctx, r.Client, remoteClusterID, resourceSlice.Name)
	if err != nil {
		klog.Errorf("Unable to get the Identity associated to resourceslice %q: %s", req.NamespacedName, err)
		return reconcile.Result{}, err
	}
	if identity.Status.KubeconfigSecretRef == nil || identity.Status.KubeconfigSecretRef.Name == "" {
		klog.V(6).Infof("Identity %q does not contain the kubeconfig secret reference yet", identity.Name)
		return reconcile.Result{}, nil
	}

	// Get associated secret.
	kubeconfigSecret, err := getters.GetKubeconfigSecretFromIdentity(ctx, r.Client, identity)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("unable to get the kubeconfig secret from identity %q: %w", identity.Name, err)
	}

	// Use the default VkOptionsTemplate reference configured on the reconciler. The VirtualNode
	// carries this ref, and the deployment-forge step fetches the template at reconcile time, so
	// template changes propagate automatically.
	vkOptsTemplateRef := r.VkOptionsDefaultTemplate

	// CreateOrUpdate the VirtualNode.
	virtualNode := forge.VirtualNode(resourceSlice.Name, resourceSlice.Namespace)
	if _, err = resource.CreateOrUpdate(ctx, r.Client, virtualNode, func() error {
		vnOpts := forge.VirtualNodeOptionsFromResourceSlice(&resourceSlice, kubeconfigSecret.Name, vkOptsTemplateRef)
		if err := forge.MutateVirtualNode(ctx, r.Client, virtualNode, identity.Spec.ClusterID, vnOpts, nil, nil, nil); err != nil {
			return err
		}
		if virtualNode.Labels == nil {
			virtualNode.Labels = map[string]string{}
		}
		virtualNode.Labels[consts.ResourceSliceNameLabelKey] = resourceSlice.Name
		return controllerutil.SetControllerReference(&resourceSlice, virtualNode, r.Scheme)
	}); err != nil {
		klog.Errorf("Unable to create or update the VirtualNode for resourceslice %q: %s", req.NamespacedName, err)
		return reconcile.Result{}, err
	}

	klog.Infof("VirtualNode created for resourceslice %q and cluster %q", req.NamespacedName, remoteClusterID)
	r.EventsRecorder.Event(&resourceSlice, "Normal", "VirtualNodeCreated", "VirtualNode created for resourceslice")
	return reconcile.Result{}, nil
}

// allConditionsAccepted checks whether all the ResourceSlice conditions are accepted.
func allConditionsAccepted(rs *authv1beta1.ResourceSlice) bool {
	authCond := liqoctlrauthentication.GetCondition(rs, authv1beta1.ResourceSliceConditionTypeAuthentication)
	authAccepted := authCond != nil && authCond.Status == authv1beta1.ResourceSliceConditionAccepted

	resourcesCond := liqoctlrauthentication.GetCondition(rs, authv1beta1.ResourceSliceConditionTypeResources)
	resourcesAccepted := resourcesCond != nil && resourcesCond.Status == authv1beta1.ResourceSliceConditionAccepted

	return authAccepted && resourcesAccepted
}

// resourceSliceConditionsAcceptedPredicate returns a predicate that only passes ResourceSlices
// whose authentication and resources conditions are both accepted.
func resourceSliceConditionsAcceptedPredicate() predicate.Funcs {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		rs, ok := obj.(*authv1beta1.ResourceSlice)
		if !ok {
			return false
		}
		return allConditionsAccepted(rs)
	})
}

// enqueueFromResourceSlice enqueues a reconcile request for the VirtualNode with the same
// name/namespace as the ResourceSlice. This is needed for ResourceSlices that have not yet
// created a VirtualNode (e.g. conditions just became accepted).
func (r *VirtualNodeReconciler) enqueueFromResourceSlice() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, o client.Object) []reconcile.Request {
			return []reconcile.Request{{
				NamespacedName: client.ObjectKeyFromObject(o),
			}}
		})
}
