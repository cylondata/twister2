//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.rsched.schedulers.k8s;

final class KubernetesConstants {

  public static final String TWISTER2_DOCKER_IMAGE = "auyar/twister2:v0.01";
  public static final String KUBERNETES_CLUSTER_TYPE = "kubernetes";
  public static final String TWISTER2_WORKER_CLASS = "K8sWorker";
  public static final String POD_SHARED_VOLUME_NAME = "twister2-shared-dir";
  public static final String POD_SHARED_VOLUME = "/twister2-shared";
  public static final String CONTAINER_NAME_PREFIX = "twister2-container-";
  public static final String TWISTER2_SERVICE_PREFIX = "twister2-service-";
  public static final String SERVICE_LABEL_PREFIX = "twister2-service-label-";

  // https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
  public static final String DELETE_OPTIONS_PROPAGATION_POLICY = "Foreground";

  private KubernetesConstants() { }
}
