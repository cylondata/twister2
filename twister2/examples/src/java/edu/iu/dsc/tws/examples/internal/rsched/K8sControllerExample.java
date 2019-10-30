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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.util.logging.Logger;

import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

public final class K8sControllerExample {
  private static final Logger LOG = Logger.getLogger(K8sControllerExample.class.getName());

  private K8sControllerExample() { }

  public static void main(String[] args) {

    String jobName = "ft-job";
    String ssName = KubernetesUtils.createWorkersStatefulSetName(jobName, 0);

    KubernetesController controller = new KubernetesController();
    controller.init("default");

    controller.patchStatefulSet(ssName, 12);
  }

}
