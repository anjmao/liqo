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

package forge

import (
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"

	liqov1beta1 "github.com/liqotech/liqo/apis/core/v1beta1"
	offloadingv1beta1 "github.com/liqotech/liqo/apis/offloading/v1beta1"
	argsutils "github.com/liqotech/liqo/pkg/utils/args"
	"github.com/liqotech/liqo/pkg/virtualKubelet/reflection/resources"
	vk "github.com/liqotech/liqo/pkg/vkMachinery"
)

// StringifyArgument returns a string representation of the key-value pair.
func StringifyArgument(key, value string) string {
	return fmt.Sprintf("%s=%s", key, value)
}

// DestringifyArgument returns the key and the value of the string representation of the key-value pair.
func DestringifyArgument(arg string) (key, value string) {
	split := strings.SplitN(arg, "=", 2)
	return split[0], split[1]
}

func getDefaultStorageClass(storageClasses []liqov1beta1.StorageType) liqov1beta1.StorageType {
	for _, storageClass := range storageClasses {
		if storageClass.Default {
			return storageClass
		}
	}
	return storageClasses[0]
}

func getDefaultIngressClass(ingressClasses []liqov1beta1.IngressType) liqov1beta1.IngressType {
	for _, ingressClass := range ingressClasses {
		if ingressClass.Default {
			return ingressClass
		}
	}
	return ingressClasses[0]
}

func getDefaultLoadBalancerClass(loadBalancerClasses []liqov1beta1.LoadBalancerType) liqov1beta1.LoadBalancerType {
	for _, loadBalancerClass := range loadBalancerClasses {
		if loadBalancerClass.Default {
			return loadBalancerClass
		}
	}
	return loadBalancerClasses[0]
}

func forgeVKContainers(
	homeCluster, remoteCluster liqov1beta1.ClusterID,
	nodeName, vkNamespace, liqoNamespace string, localPodCIDRs []string,
	storageClasses []liqov1beta1.StorageType, ingressClasses []liqov1beta1.IngressType, loadBalancerClasses []liqov1beta1.LoadBalancerType,
	opts *offloadingv1beta1.VkOptionsTemplate) []v1.Container {
	command := []string{
		"/usr/bin/virtual-kubelet",
	}

	args := []string{
		StringifyArgument(string(ForeignClusterID), string(remoteCluster)),
		StringifyArgument(string(NodeName), nodeName),
		StringifyArgument(string(NodeIP), "$(POD_IP)"),
		StringifyArgument(string(TenantNamespace), vkNamespace),
		StringifyArgument(string(LiqoNamespace), liqoNamespace),
		StringifyArgument(string(HomeClusterID), string(homeCluster)),
	}
	for i := range localPodCIDRs {
		args = append(args, StringifyArgument(string(LocalPodCIDR), localPodCIDRs[i]))
	}

	if len(storageClasses) > 0 {
		args = append(args, string(EnableStorage),
			StringifyArgument(string(RemoteRealStorageClassName),
				getDefaultStorageClass(storageClasses).StorageClassName))
	}
	if len(ingressClasses) > 0 {
		args = append(args, string(EnableIngress),
			StringifyArgument(string(RemoteRealIngressClassName),
				getDefaultIngressClass(ingressClasses).IngressClassName))
	}
	if len(loadBalancerClasses) > 0 {
		args = append(args, string(EnableLoadBalancer),
			StringifyArgument(string(RemoteRealLoadBalancerClassName),
				getDefaultLoadBalancerClass(loadBalancerClasses).LoadBalancerClassName))
	}

	args = appendArgsReflectorsWorkers(args, opts.Spec.ReflectorsConfig)
	args = appendArgsReflectorsType(args, opts.Spec.ReflectorsConfig)

	if extraAnnotations := opts.Spec.NodeExtraAnnotations; len(extraAnnotations) != 0 {
		stringifiedMap := argsutils.StringMap{StringMap: extraAnnotations}.String()
		args = append(args, StringifyArgument(string(NodeExtraAnnotations), stringifiedMap))
	}
	if extraLabels := opts.Spec.NodeExtraLabels; len(extraLabels) != 0 {
		stringifiedMap := argsutils.StringMap{StringMap: extraLabels}.String()
		args = append(args, StringifyArgument(string(NodeExtraLabels), stringifiedMap))
	}

	args = append(args, opts.Spec.ExtraArgs...)

	containerPorts := []v1.ContainerPort{}
	args = append(args, StringifyArgument(string(MetricsEnabled), strconv.FormatBool(opts.Spec.MetricsEnabled)))
	if opts.Spec.MetricsEnabled {
		args = append(args, StringifyArgument(string(MetricsAddress), opts.Spec.MetricsAddress))
		metrAddrSplit := strings.Split(opts.Spec.MetricsAddress, ":")
		metricsPort, err := strconv.ParseInt(metrAddrSplit[len(metrAddrSplit)-1], 10, 32)
		if err != nil {
			metrAddrSplit := strings.Split(vk.MetricsAddress, ":")
			// if the metrics address is not valid, use the default one
			metricsPort, _ = strconv.ParseInt(metrAddrSplit[len(metrAddrSplit)-1], 10, 32)
		}
		containerPorts = append(containerPorts, v1.ContainerPort{
			Name:          "metrics",
			ContainerPort: int32(metricsPort),
			Protocol:      v1.ProtocolTCP,
		})
	}

	pullPolicy := v1.PullIfNotPresent
	if opts.Spec.PullPolicy != "" {
		pullPolicy = opts.Spec.PullPolicy
	}

	return []v1.Container{
		{
			Name:            vk.ContainerName,
			Resources:       opts.Spec.Resources,
			Image:           opts.Spec.ContainerImage,
			ImagePullPolicy: pullPolicy,
			Command:         command,
			Args:            args,
			Env: []v1.EnvVar{
				{
					Name:      "POD_IP",
					ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "status.podIP"}},
				},
				{
					Name:      "POD_NAME",
					ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.name"}},
				},
				{
					Name:      "POD_NAMESPACE",
					ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{FieldPath: "metadata.namespace"}},
				},
				{
					Name:  "VIRTUALNODE_NAME",
					Value: nodeName,
				},
			},
			Ports: containerPorts,
		},
	}
}

func forgeVKPodSpec(vkNamespace string, homeCluster liqov1beta1.ClusterID, liqoNamespace string, localPodCIDRs []string,
	virtualNode *offloadingv1beta1.VirtualNode, opts *offloadingv1beta1.VkOptionsTemplate) v1.PodSpec {
	containers := forgeVKContainers(
		homeCluster, virtualNode.Spec.ClusterID,
		virtualNode.Name, vkNamespace, liqoNamespace, localPodCIDRs,
		virtualNode.Spec.StorageClasses, virtualNode.Spec.IngressClasses, virtualNode.Spec.LoadBalancerClasses,
		opts)

	// Inject the VirtualNode-spec-derived args (kubeconfig secret, create-node, node-check-network).
	// These were previously injected by the mutating webhook into the stored deployment template.
	if len(containers) > 0 {
		containers[0].Args = appendVirtualNodeArgs(containers[0].Args, virtualNode)
	}

	return v1.PodSpec{
		Containers:         containers,
		ServiceAccountName: virtualNode.Name,
		ImagePullSecrets:   opts.Spec.ImagePullSecrets,
		Tolerations:        opts.Spec.Tolerations,
	}
}

// appendVirtualNodeArgs appends (or updates) the VirtualNode-spec-derived container args:
// --foreign-kubeconfig-secret-name, --create-node, --node-check-network.
func appendVirtualNodeArgs(args []string, virtualNode *offloadingv1beta1.VirtualNode) []string {
	if virtualNode.Spec.KubeconfigSecretRef != nil {
		args = setOrAppendArg(args, string(ForeignClusterKubeconfigSecretName), virtualNode.Spec.KubeconfigSecretRef.Name)
	}
	if virtualNode.Spec.CreateNode != nil {
		args = setOrAppendArg(args, string(CreateNode), strconv.FormatBool(*virtualNode.Spec.CreateNode))
	}
	if virtualNode.Spec.DisableNetworkCheck != nil {
		args = setOrAppendArg(args, string(NodeCheckNetwork), strconv.FormatBool(!*virtualNode.Spec.DisableNetworkCheck))
	}
	return args
}

// setOrAppendArg sets the value of an argument (key=value), replacing the existing one if present,
// or appending a new one if not.
func setOrAppendArg(args []string, key, value string) []string {
	argVal := StringifyArgument(key, value)
	for i, arg := range args {
		if strings.HasPrefix(arg, key+"=") || arg == key {
			args[i] = argVal
			return args
		}
	}
	return append(args, argVal)
}

func appendArgsReflectorsWorkers(args []string, reflectorsConfig map[string]offloadingv1beta1.ReflectorConfig) []string {
	if reflectorsConfig == nil {
		return args
	}

	for _, resource := range resources.Reflectors {
		reflector, ok := reflectorsConfig[string(resource)]
		if !ok {
			continue
		}
		key := fmt.Sprintf("--%s-reflection-workers", resource)
		args = append(args, StringifyArgument(key, strconv.Itoa(int(reflector.NumWorkers))))
	}

	return args
}

func appendArgsReflectorsType(args []string, reflectorsConfig map[string]offloadingv1beta1.ReflectorConfig) []string {
	if reflectorsConfig == nil {
		return args
	}

	for _, resource := range resources.ReflectorsCustomizableType {
		reflector, ok := reflectorsConfig[string(resource)]
		if !ok {
			continue
		}
		key := fmt.Sprintf("--%s-reflection-type", resource)
		args = append(args, StringifyArgument(key, string(reflector.Type)))
	}

	return args
}
