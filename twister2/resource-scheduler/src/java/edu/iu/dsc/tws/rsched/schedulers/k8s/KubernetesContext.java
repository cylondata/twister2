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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class KubernetesContext extends SchedulerContext {

  public static final int CONTAINERS_PER_POD_DEFAULT = 1;
  public static final String CONTAINERS_PER_POD = "kubernetes.containers_per_pod";

  public static final String KUBERNETES_NAMESPACE_DEFAULT = "default";
  public static final String KUBERNETES_NAMESPACE = "kubernetes.namespace";

  public static final int SERVICE_PORT_DEFAULT = 11111;
  public static final String SERVICE_PORT = "kubernetes.service.port";

  public static final int SERVICE_TARGET_PORT_DEFAULT = 33333;
  public static final String SERVICE_TARGET_PORT = "kubernetes.service.target.port";

  public static final int K8S_WORKER_BASE_PORT_DEFAULT = 9999;
  public static final String K8S_WORKER_BASE_PORT = "kubernetes.worker.base.port";

  public static final String K8S_IMAGE_PULL_POLICY_NAMESPACE = "IfNotPresent";
  public static final String K8S_IMAGE_PULL_POLICY = "kubernetes.image.pull.policy";

  public static final String K8S_PERSISTENT_STORAGE_CLASS_DEFAULT = "twister2";
  public static final String K8S_PERSISTENT_STORAGE_CLASS = "kubernetes.persistent.storage.class";

  public static final String K8S_STORAGE_ACCESS_MODE_DEFAULT = "ReadWriteMany";
  public static final String K8S_STORAGE_ACCESS_MODE = "kubernetes.storage.access.mode";

  public static final String K8S_STORAGE_RECLAIM_POLICY_DEFAULT = "Retain";
  public static final String K8S_STORAGE_RECLAIM_POLICY = "kubernetes.storage.reclaim.policy";

  public static int containersPerPod(Config cfg) {
    return cfg.getIntegerValue(CONTAINERS_PER_POD, CONTAINERS_PER_POD_DEFAULT);
  }

  public static String namespace(Config cfg) {
    return cfg.getStringValue(KUBERNETES_NAMESPACE, KUBERNETES_NAMESPACE_DEFAULT);
  }

  public static int servicePort(Config cfg) {
    return cfg.getIntegerValue(SERVICE_PORT, SERVICE_PORT_DEFAULT);
  }

  public static int serviceTargetPort(Config cfg) {
    return cfg.getIntegerValue(SERVICE_TARGET_PORT, SERVICE_TARGET_PORT_DEFAULT);
  }

  public static int workerBasePort(Config cfg) {
    return cfg.getIntegerValue(K8S_WORKER_BASE_PORT, K8S_WORKER_BASE_PORT_DEFAULT);
  }

  public static String imagePullPolicy(Config cfg) {
    return cfg.getStringValue(K8S_IMAGE_PULL_POLICY, K8S_IMAGE_PULL_POLICY_NAMESPACE);
  }

  public static String persistentStorageClass(Config cfg) {
    return cfg.getStringValue(K8S_PERSISTENT_STORAGE_CLASS, K8S_PERSISTENT_STORAGE_CLASS_DEFAULT);
  }

  public static String storageAccessMode(Config cfg) {
    return cfg.getStringValue(K8S_STORAGE_ACCESS_MODE, K8S_STORAGE_ACCESS_MODE_DEFAULT);
  }

  public static String storageReclaimPolicy(Config cfg) {
    return cfg.getStringValue(K8S_STORAGE_RECLAIM_POLICY, K8S_STORAGE_RECLAIM_POLICY_DEFAULT);
  }

}
