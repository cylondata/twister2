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
  public static final String CONTAINERS_PER_POD = "kubernetes.containers.per.pod";

  public static final String KUBERNETES_DEFAULT_NAMESPACE = "default";
  public static final String KUBERNETES_NAMESPACE = "kubernetes.namespace";

  public static final int SERVICE_PORT_DEFAULT = 11111;
  public static final String SERVICE_PORT = "kubernetes.service.port";

  public static final int SERVICE_TARGET_PORT_DEFAULT = 33333;
  public static final String SERVICE_TARGET_PORT = "kubernetes.service.target.port";

  public static int containersPerPod(Config cfg) {
    return cfg.getIntegerValue(CONTAINERS_PER_POD, CONTAINERS_PER_POD_DEFAULT);
  }

  public static String namespace(Config cfg) {
    return cfg.getStringValue(KUBERNETES_NAMESPACE, KUBERNETES_DEFAULT_NAMESPACE);
  }

  public static int servicePort(Config cfg) {
    return cfg.getIntegerValue(SERVICE_PORT, SERVICE_PORT_DEFAULT);
  }

  public static int serviceTargetPort(Config cfg) {
    return cfg.getIntegerValue(SERVICE_TARGET_PORT, SERVICE_TARGET_PORT_DEFAULT);
  }

}
