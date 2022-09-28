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
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
)

// DefaultWaitTime is 60s if ScheduleTimeoutSeconds is not specified.
const DefaultWaitTime = 60 * time.Second

// GetPodGroupLabel get pod group from pod annotations
func GetPodLabel(pod *v1.Pod, label_name string) string {
	return pod.Labels[label_name]

}

// GetPodGroupLabel get pod group from pod annotations
func GetSchedulerName(pod *v1.Pod) string {
	return pod.Spec.SchedulerName
}

// GetPodGroupLabel get pod group from pod annotations
func GetAnnotationByName(pod *v1.Pod, key string) string {
	for k, v := range pod.ObjectMeta.Annotations {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}
