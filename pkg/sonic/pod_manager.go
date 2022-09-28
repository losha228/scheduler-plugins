package sonic

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

import (
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	appInformerv1 "k8s.io/client-go/informers/app/v1"
	informerv1 "k8s.io/client-go/informers/core/v1"
	appListerv1 "k8s.io/client-go/listers/app/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Status string

const (
	Success Status = "Success"
	Wait    Status = "Wait"
)

// PodGroupManager defines the scheduling operation called
type PodManager struct {
	// snapshotSharedLister is pod shared list
	snapshotSharedLister framework.SharedLister
	// scheduleTimeout is the default timeout for podgroup scheduling.
	// If podgroup's scheduleTimeoutSeconds is set, it will be used.
	scheduleTimeout *time.Duration
	// podLister is pod lister
	podLister listerv1.PodLister
	// podLister is pod lister
	dsLister appListerv1.PodLister
	// reserveResourcePercentage is the reserved resource for the max finished group, range (0,100]
	reserveResourcePercentage int32
	sync.RWMutex
}

// NewPodGroupManager creates a new operation object.
func NewPodManager(snapshotSharedLister framework.SharedLister, scheduleTimeout *time.Duration,
	podInformer informerv1.PodInformer, dsInformer appInformerv1.DaemonSetInformer) *PodManager {
	podMgr := &PodManager{
		snapshotSharedLister: snapshotSharedLister,
		scheduleTimeout:      scheduleTimeout,
		podLister:            podInformer.Lister(),
		dsLister:             dsInformer.Lister(),
	}
	return podMgr
}

// GetNamespacedName returns the namespaced name.
func (pm *PodManager) GetPodCount(ns string) int {
	pods, err := pm.podLister.Pods(ns).List(
		labels.SelectorFromSet(labels.Set{"test": "liveness"}),
	)
	if err != nil {
		klog.ErrorS(err, "Failed to obtain pods with label, test:liveness")
		return 0
	}

	return len(pods)
}

// GetNamespacedName returns the namespaced name.
func GetNamespacedName(obj metav1.Object) string {
	return fmt.Sprintf("%v/%v", obj.GetNamespace(), obj.GetName())
}
