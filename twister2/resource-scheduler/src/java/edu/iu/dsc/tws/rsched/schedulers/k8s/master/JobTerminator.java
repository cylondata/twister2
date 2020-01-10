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
package edu.iu.dsc.tws.rsched.schedulers.k8s.master;

import java.util.ArrayList;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

public class JobTerminator implements IJobTerminator {

  private KubernetesController controller;
  private Config config;

  public JobTerminator(Config config) {
    this.config = config;

    controller = new KubernetesController();
    String namespace = KubernetesContext.namespace(config);
    controller.init(namespace);
  }

  @Override
  public boolean terminateJob(String jobID) {

    // delete the StatefulSets for workers
    ArrayList<String> ssNameLists = controller.getStatefulSetsForJobWorkers(jobID);
    boolean ssForWorkersDeleted = true;
    for (String ssName: ssNameLists) {
      ssForWorkersDeleted &= controller.deleteStatefulSet(ssName);
    }

    // delete the job service
    String serviceName = KubernetesUtils.createServiceName(jobID);
    boolean serviceForWorkersDeleted = controller.deleteService(serviceName);

    // delete the persistent volume claim
    String pvcName = KubernetesUtils.createPersistentVolumeClaimName(jobID);
    boolean pvcDeleted = controller.deletePersistentVolumeClaim(pvcName);

    // delete the job master StatefulSet
    String jobMasterStatefulSetName = KubernetesUtils.createJobMasterStatefulSetName(jobID);
    boolean ssForJobMasterDeleted =
        controller.deleteStatefulSet(jobMasterStatefulSetName);

    // delete the job master service
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobID);
    boolean serviceForJobMasterDeleted = controller.deleteService(jobMasterServiceName);

    return ssForWorkersDeleted
        && serviceForWorkersDeleted
        && serviceForJobMasterDeleted
        && pvcDeleted
        && ssForJobMasterDeleted;
  }
}
