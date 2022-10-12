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

package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformer "k8s.io/client-go/informers/apps/v1"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	appslister "k8s.io/client-go/listers/apps/v1"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"sigs.k8s.io/scheduler-plugins/pkg/util"
)

// SonicDeamonsetController is a controller that process sonic deamonset
type SonicDeamonsetController struct {
	podQueue        workqueue.RateLimitingInterface
	podLister       corelister.PodLister
	podListerSynced cache.InformerSynced
	dpLister        appslister.DeploymentLister
	dpListerSynced  cache.InformerSynced
	kubeClient      kubernetes.Interface
}

const (
	PostCheckDoneTag   = "PostCheckDone"
	PostCheckNeededTag = "PostCheckNeeded"
	// PausedDeployReason is added in a deployment when it is paused. Lack of progress shouldn't be
	// estimated once a deployment is paused.
	PausedDeployReason = "DeploymentPaused"
)

// NewSonicDeamonsetController returns a new *SonicDeamonsetController
func NewSonicDeamonsetController(client kubernetes.Interface,
	podInformer coreinformer.PodInformer,
	dpInformer appsinformer.DeploymentInformer) *SonicDeamonsetController {

	ctrl := &SonicDeamonsetController{
		podQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "PodWork"),
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.podAdded,
		UpdateFunc: ctrl.podUpdated,
	})

	ctrl.kubeClient = client
	ctrl.podLister = podInformer.Lister()
	ctrl.podListerSynced = podInformer.Informer().HasSynced
	ctrl.dpLister = dpInformer.Lister()
	ctrl.dpListerSynced = dpInformer.Informer().HasSynced
	return ctrl
}

// Run starts listening on channel events
func (ctrl *SonicDeamonsetController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.podQueue.ShutDown()

	klog.InfoS("Starting sonic pod controller")
	ctrl.log("Run", "Starting sonic pod controller", nil)
	defer klog.InfoS("Shutting sonic pod controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.podListerSynced) {
		klog.ErrorS(nil, "Cannot sync caches")
		return
	}
	klog.InfoS("Pod sync finished")
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.worker, time.Second, stopCh)
	}

	<-stopCh
}

// podAdded reacts to a PG creation
func (ctrl *SonicDeamonsetController) podAdded(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	pod := obj.(*v1.Pod)

	ctrl.log("podAdded", fmt.Sprintf("PodCreate event got: pod %v in namespace %v is added", pod.Name, pod.Namespace), pod)
	// check post check tag
	_, postCheckNeedfound := pod.Annotations[PostCheckNeededTag]
	_, postCheckDonefound := pod.Annotations[PostCheckDoneTag]
	if postCheckDonefound || !postCheckNeedfound {
		ctrl.log("podAdded", fmt.Sprintf("Ignore pod %v in namespace %v because either PostCheck was done or no PostCheckNeeded", pod.Name, pod.Namespace), pod)
		return
	}

	ctrl.log("podAdded", fmt.Sprintf("pod %v in namespace %v, postCheckDonefound=%v, postCheckNeedfound=%v", pod.Name, pod.Namespace, postCheckDonefound, postCheckNeedfound), pod)
	ctrl.log("podAdded", fmt.Sprintf("Enqueue %v", key), pod)
	ctrl.podQueue.Add(key)

	klog.V(5).InfoS("Add pod group when pod gets added", "podGroup", klog.KObj(pod), "pod", klog.KObj(pod))
}

// pgUpdated reacts to a PG update
func (ctrl *SonicDeamonsetController) podUpdated(old, new interface{}) {
	pod := old.(*v1.Pod)
	ctrl.log("podUpdated", fmt.Sprintf("PodUpdate event got: pod %v in namespace %v ", pod.Name, pod.Namespace), pod)
	ctrl.podAdded(new)
}

func (ctrl *SonicDeamonsetController) worker() {
	for ctrl.processNextWorkItem() {
	}
}

// processNextWorkItem deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *SonicDeamonsetController) processNextWorkItem() bool {
	keyObj, quit := ctrl.podQueue.Get()
	if quit {
		return false
	}
	defer ctrl.podQueue.Done(keyObj)

	key, ok := keyObj.(string)
	if !ok {
		ctrl.podQueue.Forget(keyObj)
		runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", keyObj))
		return true
	}
	if err := ctrl.syncHandler(key); err != nil {
		runtime.HandleError(err)
		klog.ErrorS(err, "Error syncing sonic pod", "SonicPod", key)
		return true
	}
	return true
}

// syncHandle syncs pod group and convert status
func (ctrl *SonicDeamonsetController) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}
	defer func() {
		if err != nil {
			ctrl.podQueue.AddRateLimited(key)
			return
		}
	}()
	pod, err := ctrl.podLister.Pods(namespace).Get(name)
	if apierrs.IsNotFound(err) {
		klog.V(5).InfoS("Pod has been deleted", "SonicPod", key)
		return nil
	}
	if err != nil {
		klog.V(3).ErrorS(err, "Unable to retrieve pod from store", "SonicPod", key)
		return err
	}

	ctrl.log("syncHandler", fmt.Sprintf("Do post-check for pod %v in namespace %v", pod.Name, pod.Namespace), pod)
	ctrl.log("syncHandler", fmt.Sprintf("Update tag for pod %v in namespace %v", pod.Name, pod.Namespace), pod)

	// simulate the post check fail
	_, failed := pod.Annotations["PostCheckFail"]
	if failed {
		ctrl.log("syncHandler", fmt.Sprintf("Post-check is failed for pod %v in namespace %v", pod.Name, pod.Namespace), pod)
		err = ctrl.pauseDeployment(pod)
		if err != nil {
			ctrl.log("syncHandler", fmt.Sprintf("Failed to pause deployment for pod %v in namespace %v", pod.Name, pod.Namespace), pod)
		} else {
			ctrl.podQueue.Forget(key)
			return nil
		}
	}

	podCopy := pod.DeepCopy()
	podCopy.Annotations["PostCheckDone"] = "true"
	err = ctrl.patchPod(pod, podCopy)
	if err == nil {
		ctrl.podQueue.Forget(key)
		ctrl.log("syncHandler", fmt.Sprintf("Post-check is done for pod %v in namespace %v", pod.Name, pod.Namespace), pod)
	}
	return err
}

func (ctrl *SonicDeamonsetController) patchPod(old, new *v1.Pod) error {
	if !reflect.DeepEqual(old, new) {
		patch, err := util.CreateMergePatch(old, new)
		if err != nil {
			return err
		}

		_, err = ctrl.kubeClient.CoreV1().Pods(old.Namespace).Patch(context.TODO(), old.Name, types.MergePatchType,
			patch, metav1.PatchOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// Pause depoloyment when post check is failed
func (ctrl *SonicDeamonsetController) pauseDeployment(pod *v1.Pod) error {
	if pod == nil {
		return fmt.Errorf("Pod is invalid")
	}

	var refs []string
	var deploymentStr string
	for _, ownerRef := range pod.OwnerReferences {
		refs = append(refs, fmt.Sprintf("%s/%s", pod.Namespace, ownerRef.Name))
		deploymentStr = ownerRef.Name
	}

	// get deployment from pod OwnerReferences
	splitStr := strings.Split(deploymentStr, "-")
	deploymentName := strings.Join(splitStr[0:len(splitStr)-2], "-")
	d, err := ctrl.kubeClient.AppsV1().Deployments(pod.Namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		klog.ErrorS(err, fmt.Sprintf("deployment %v is not found", deploymentName))
		return err
	}

	ctrl.log("getDeploymentForPod", fmt.Sprintf("pod %v is controlled by deployment %v", pod.Name, klog.KObj(d)), pod)
	cond := ctrl.getDeploymentCondition(d.Status, appsv1.DeploymentProgressing)
	pausedCondExists := cond != nil && cond.Reason == PausedDeployReason

	if !pausedCondExists {
		condition := ctrl.newDeploymentCondition(appsv1.DeploymentProgressing, v1.ConditionUnknown, PausedDeployReason, "Deployment is paused")
		ctrl.setDeploymentCondition(&d.Status, *condition)
	} else {
		return nil
	}

	ctrl.log("getDeploymentForPod", fmt.Sprintf("Pause the deployment %v, %v", d.Name, klog.KObj(d)), pod)
	_, err = ctrl.kubeClient.AppsV1().Deployments(d.Namespace).UpdateStatus(context.TODO(), d, metav1.UpdateOptions{})
	return err
}

func (ctrl *SonicDeamonsetController) log(method, msg string, pod *v1.Pod) {
	klog.ErrorS(nil, msg+" pod: ", klog.KObj(pod))
}

// NewDeploymentCondition creates a new deployment condition.
func (ctrl *SonicDeamonsetController) newDeploymentCondition(condType appsv1.DeploymentConditionType, status v1.ConditionStatus, reason, message string) *appsv1.DeploymentCondition {
	return &appsv1.DeploymentCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// GetDeploymentCondition returns the condition with the provided type.
func (ctrl *SonicDeamonsetController) getDeploymentCondition(status appsv1.DeploymentStatus, condType appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetDeploymentCondition updates the deployment to include the provided condition. If the condition that
// we are about to add already exists and has the same status and reason then we are not going to update.
func (ctrl *SonicDeamonsetController) setDeploymentCondition(status *appsv1.DeploymentStatus, condition appsv1.DeploymentCondition) {
	currentCond := ctrl.getDeploymentCondition(*status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}
	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}
	newConditions := ctrl.filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// filterOutCondition returns a new slice of deployment conditions without conditions with the provided type.
func (ctrl *SonicDeamonsetController) filterOutCondition(conditions []appsv1.DeploymentCondition, condType appsv1.DeploymentConditionType) []appsv1.DeploymentCondition {
	var newConditions []appsv1.DeploymentCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}
