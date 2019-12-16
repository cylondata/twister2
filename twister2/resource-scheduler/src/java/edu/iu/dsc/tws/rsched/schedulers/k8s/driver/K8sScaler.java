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
package edu.iu.dsc.tws.rsched.schedulers.k8s.driver;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.driver.IScalerPerCluster;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

public class K8sScaler implements IScalerPerCluster {

  private static final Logger LOG = Logger.getLogger(K8sScaler.class.getName());

  private Config config;
  private KubernetesController k8sController;

  // values for scalable ComputeResource in the job
  private String scalableSSName;
  private int replicas;
  private int workersPerPod;
  private boolean scalable;

  public K8sScaler(Config config, JobAPI.Job job, KubernetesController k8sController) {
    this.k8sController = k8sController;
    this.config = config;

    int computeResourceIndex = job.getComputeResourceCount() - 1;
    JobAPI.ComputeResource scalableCompRes = job.getComputeResource(computeResourceIndex);

    replicas = scalableCompRes.getInstances();
    workersPerPod = scalableCompRes.getWorkersPerPod();
    scalable = scalableCompRes.getScalable();
    scalableSSName =
        KubernetesUtils.createWorkersStatefulSetName(job.getJobId(), computeResourceIndex);
  }

  @Override
  public boolean isScalable() {
    // if there is no scalable compute resource in the job, can not be scalable
    if (!scalable) {
      return false;
    }

    // if it is an OpenMPI job, it is not scalable
    if (SchedulerContext.useOpenMPI(config)) {
      return false;
    }

    return true;
  }

  /**
   * add new workers to the scalable compute resource
   * @return
   */
  @Override
  public boolean scaleUpWorkers(int instancesToAdd) {

    if (instancesToAdd % workersPerPod != 0) {
      LOG.severe("instancesToAdd has to be a multiple of workersPerPod=" + workersPerPod);
      return false;
    }

    int podsToAdd = instancesToAdd / workersPerPod;

    boolean scaledUp = k8sController.patchStatefulSet(scalableSSName, replicas + podsToAdd);
    if (!scaledUp) {
      return false;
    }

    replicas = replicas + podsToAdd;

    return true;
  }

  /**
   * remove workers from the scalable compute resource
   * @param instancesToRemove
   * @return
   */
  @Override
  public boolean scaleDownWorkers(int instancesToRemove) {

    if (instancesToRemove % workersPerPod != 0) {
      LOG.severe("instancesToRemove has to be a multiple of workersPerPod=" + workersPerPod);
      return false;
    }

    int podsToRemove = instancesToRemove / workersPerPod;

    if (podsToRemove > replicas) {
      LOG.severe(String.format("There are %d instances of scalable ComputeResource, "
          + "and %d instances requested to be removed", replicas, podsToRemove));
      return false;
    }

    boolean scaledDown = k8sController.patchStatefulSet(scalableSSName, replicas - podsToRemove);
    if (!scaledDown) {
      return false;
    }

    // update replicas
    replicas = replicas - podsToRemove;

    return true;
  }

}
