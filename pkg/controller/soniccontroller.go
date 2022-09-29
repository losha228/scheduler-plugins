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
	"time"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
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
	pgListerSynced  cache.InformerSynced
	podListerSynced cache.InformerSynced
	kubeClient      kubernetes.Interface
}

// NewSonicDeamonsetController returns a new *SonicDeamonsetController
func NewSonicDeamonsetController(client kubernetes.Interface,
	podInformer coreinformer.PodInformer) *SonicDeamonsetController {

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
	_, found := pod.Annotations["PostCheckNeeded"]
	if !found {
		ctrl.log("podAdded", fmt.Sprintf("pod %v in namespace %v has no PostCheckNeeded tag", pod.Name, pod.Namespace), pod)
		return
	} else {
		ctrl.log("podAdded", fmt.Sprintf("Enqueue %v", key), pod)
		ctrl.podQueue.Add(key)
	}
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

func (ctrl *SonicDeamonsetController) log(method, msg string, pod *v1.Pod) {
	klog.ErrorS(nil, msg+" pod: ", klog.KObj(pod))
}
