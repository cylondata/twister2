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

public final class KubernetesConstants {

  public static final String KUBERNETES_CLUSTER_TYPE = "kubernetes";
  public static final String POD_MEMORY_VOLUME_NAME = "twister2-memory-dir";
  public static final String POD_MEMORY_VOLUME = "/twister2-memory-dir";
  public static final String POD_VOLATILE_VOLUME_NAME = "twister2-volatile-dir";
  public static final String POD_VOLATILE_VOLUME = "/twister2-volatile";
  public static final String CONTAINER_NAME_PREFIX = "twister2-container-";
  public static final String TWISTER2_SERVICE_PREFIX = "twister2-service-";
  public static final String SERVICE_LABEL_PREFIX = "twister2-service-label-";
  public static final String SERVICE_LABEL_KEY = "app";
  public static final String TWISTER2_JOB_PODS_KEY = "twister2-job-pods";
  public static final String TWISTER2_JOB_PODS_PREFIX = "twister2-";
  public static final String TWISTER2_STORAGE_CLAIM_PREFIX = "twister2-storage-";
  public static final String PERSISTENT_VOLUME_NAME = "persistent-volume";
  public static final String PERSISTENT_VOLUME_MOUNT = "/persistent";
  public static final String SECRET_VOLUME_NAME = "kube-openmpi-ssh-key";
  public static final String SECRET_VOLUME_MOUNT = "/ssh-key/openmpi";

  // https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
  public static final String DELETE_OPTIONS_PROPAGATION_POLICY = "Foreground";

  private KubernetesConstants() { }
}
