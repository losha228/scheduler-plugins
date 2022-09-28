/*
Copyright 2020 The Kubernetes Authors.

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

package sonic

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/scheduler-plugins/apis/scheduling"
)

// SonicScheduling is a plugin that inject sonice device precheck/postcheck and device lock logic during creating pod.
type SonicScheduling struct {
	frameworkHandler framework.Handle
	podMgr           *PodManager
	scheduleTimeout  *time.Duration
}

var _ framework.PreFilterPlugin = &SonicScheduling{}
var _ framework.PostFilterPlugin = &SonicScheduling{}
var _ framework.PermitPlugin = &SonicScheduling{}
var _ framework.ReservePlugin = &SonicScheduling{}
var _ framework.PreBindPlugin = &SonicScheduling{}
var _ framework.PostBindPlugin = &SonicScheduling{}
var _ framework.EnqueueExtensions = &SonicScheduling{}

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = "SonicScheduling"
)

// New initializes and returns a new sonicscheduling plugin.
func New(obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	podInformer := handle.SharedInformerFactory().Core().V1().Pods()
	dsInformer := handle.SharedInformerFactory().Apps().V1().DaemonSets()
	scheduleTimeDuration := time.Duration(10) * time.Second

	podMgr := NewPodManager(handle.SnapshotSharedLister(), &scheduleTimeDuration, podInformer, dsInformer)
	plugin := &SonicScheduling{
		frameworkHandler: handle,
		podMgr:           podMgr,
		scheduleTimeout:  &scheduleTimeDuration,
	}

	return plugin, nil
}

func (ss *SonicScheduling) EventsToRegister() []framework.ClusterEvent {
	// To register a custom event, follow the naming convention at:
	// https://git.k8s.io/kubernetes/pkg/scheduler/eventhandlers.go#L403-L410
	pgGVK := fmt.Sprintf("podgroups.v1alpha1.%v", scheduling.GroupName)
	return []framework.ClusterEvent{
		{Resource: framework.Pod, ActionType: framework.Add},
		{Resource: framework.GVK(pgGVK), ActionType: framework.Add | framework.Update},
	}
}

// Name returns name of the plugin. It is used in logs, etc.
func (ss *SonicScheduling) Name() string {
	return Name
}

// PreFilter performs the following validations.
// 1. Whether the PodGroup that the Pod belongs to is on the deny list.
// 2. Whether the total number of pods in a PodGroup is less than its `minMember`.
func (ss *SonicScheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	// If PreFilter fails, return framework.UnschedulableAndUnresolvable to avoid
	// any preemption attempts.

	ss.log("PreFilter", "PreFilter is called", pod, "")
	value := GetAnnotationByName(pod, "PreFilter")
	if strings.Contains(value, "fail") {
		ss.log("PreFilter", "set it to Unschedulable", pod, "")
		return nil, framework.NewStatus(framework.Unschedulable, "")
	}

	return nil, framework.NewStatus(framework.Success, "")
}

// PostFilter is used to reject pods if a pod does not pass PreFilter or Filter.
func (ss *SonicScheduling) PostFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod,
	filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	ss.log("PostFilter", "PostFilter is called for pod ", pod, "")
	count := ss.podMgr.GetPodCount(pod.Namespace)
	return &framework.PostFilterResult{}, framework.NewStatus(framework.Unschedulable,
		fmt.Sprintf("There are %v pod in namespace %v,Pod %v is unschedulable even after PostFilter", count, pod.Namespace, pod.Name))
}

// PreFilterExtensions returns a PreFilterExtensions interface if the plugin implements one.
func (ss *SonicScheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Permit is the functions invoked by the framework at "Permit" extension point.
func (ss *SonicScheduling) Permit(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	waitTime := *ss.scheduleTimeout
	var retStatus *framework.Status

	retStatus = framework.NewStatus(framework.Success, "")
	waitTime = 0
	ss.log("Permit", "Permit is called for pod for precheck", pod, nodeName)
	value := GetAnnotationByName(pod, "Permit")

	if strings.Contains(value, "wait") {
		ss.log("Permit", fmt.Sprintf("set it to Wait=%v", waitTime), pod, nodeName)
		retStatus = framework.NewStatus(framework.Wait, "")
	} else if strings.Contains(value, "error") {
		ss.log("Permit", "set it to Wait", pod, nodeName)
		retStatus = framework.NewStatus(framework.Error, "")
	} else if strings.Contains(value, "Unschedulable") {
		ss.log("Permit", "set it to Unschedulable", pod, nodeName)
		retStatus = framework.NewStatus(framework.Unschedulable, "")
	}
	return retStatus, waitTime
}

// Reserve is the functions invoked by the framework at "reserve" extension point.
func (ss *SonicScheduling) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	ss.log("Reserve", "Reserve is called for pod", pod, nodeName)
	return nil
}

// Unreserve
func (ss *SonicScheduling) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	ss.log("Unreserve", "Unreserve is called for pod", pod, nodeName)
	return
}

// PostBind is called after a pod is successfully bound.
func (ss *SonicScheduling) PreBind(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	ss.log("PreBind", "pod pre-upgrade: lock the device ", pod, nodeName)
	value := GetAnnotationByName(pod, "PreBind")
	retStatus := framework.NewStatus(framework.Success, "")
	if strings.Contains(value, "wait") {
		ss.log("PreBind", "set it to Wait", pod, nodeName)
		retStatus = framework.NewStatus(framework.Wait, "")
	} else if strings.Contains(value, "error") {
		ss.log("PreBind", "set it to Wait", pod, nodeName)
		retStatus = framework.NewStatus(framework.Error, "")
	} else if strings.Contains(value, "Unschedulable") {
		ss.log("PreBind", "set it to Unschedulable", pod, nodeName)
		retStatus = framework.NewStatus(framework.Unschedulable, "")
	}
	return retStatus
}

// PostBind is called after a pod is successfully bound.
func (ss *SonicScheduling) PostBind(ctx context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) {
	ss.log("PostBind", "pod post-upgrade: release device lock", pod, nodeName)
}

func (ss *SonicScheduling) log(method, msg string, pod *v1.Pod, nodeName string) {
	klog.ErrorS(nil, method, msg+" pod: ", klog.KObj(pod))
}
