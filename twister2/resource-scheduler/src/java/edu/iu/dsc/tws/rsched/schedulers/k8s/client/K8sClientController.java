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
package edu.iu.dsc.tws.rsched.schedulers.k8s.client;

import java.util.ArrayList;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.client.IClientController;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.sclient.JMSubmittingClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.JobPackageTransferThread;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

public class K8sClientController implements IClientController {

  private static final Logger LOG = Logger.getLogger(K8sClientController.class.getName());

  private JMSubmittingClient submittingClient;
  private JobAPI.Job job;
  private KubernetesController k8sController;
  private String scalableSSName;
  private Config config;
  private String jobPackageFile;
  private int scalableSSReplicas;
  private int workersPerPod;

  public K8sClientController(Config config, String jmHost, JobAPI.Job job, String jobPackageFile,
                             KubernetesController k8sController) {
    this.config = config;
    this.job = job;
    this.jobPackageFile = jobPackageFile;
    this.k8sController = k8sController;

    this.scalableSSReplicas =
        job.getComputeResource(job.getComputeResourceCount() - 1).getInstances();

    this.workersPerPod =
        job.getComputeResource(job.getComputeResourceCount() - 1).getWorkersPerPod();

    scalableSSName = KubernetesUtils.createWorkersStatefulSetName(
        job.getJobName(), job.getComputeResourceCount() - 1);

    int jmPort = JobMasterContext.jobMasterPort(config);
    submittingClient = new JMSubmittingClient(config, jmHost, jmPort);
  }

  /**
   * add new workers to the scalable compute resource
   * @return
   */
  @Override
  public boolean scaleUpWorkers(int instancesToAdd) {

    if (instancesToAdd <= 0) {
      LOG.severe("instancesToAdd has to be a positive integer");
      return false;
    }

    if (instancesToAdd % workersPerPod != 0) {
      LOG.severe("instancesToAdd has to be a multiple of workersPerPod=" + workersPerPod);
      return false;
    }

    int podsToAdd = instancesToAdd / workersPerPod;

    // if the client is uploading the job package, start upload threads
    if (KubernetesContext.clientToPodsUploading(config)) {
      ArrayList<String> podNames = generatePodNames(podsToAdd);
      String namespace = KubernetesContext.namespace(config);
      JobPackageTransferThread.startTransferThreadsForScaledUpPods(
          namespace, podNames, jobPackageFile);
    }

    boolean scaledUp =
        k8sController.patchStatefulSet(scalableSSName, scalableSSReplicas + podsToAdd);
    if (!scaledUp) {
      return false;
    }

    // complete the uploading
    if (KubernetesContext.clientToPodsUploading(config)) {
      boolean uploaded = JobPackageTransferThread.completeFileTransfers();

      // if scaling up pods is successful but uploading is unsuccessful,
      // scale down again
      if (!uploaded) {
        k8sController.patchStatefulSet(scalableSSName, scalableSSReplicas);
        return false;
      }
    }

    scalableSSReplicas = scalableSSReplicas + podsToAdd;
    return true;
  }

  /**
   * remove workers from the scalable compute resource
   * @param instancesToRemove
   * @return
   */
  @Override
  public boolean scaleDownWorkers(int instancesToRemove) {
    if (instancesToRemove <= 0) {
      LOG.severe("instancesToRemove has to be a positive integer");
      return false;
    }

    if (instancesToRemove % workersPerPod != 0) {
      LOG.severe("instancesToRemove has to be a multiple of workersPerPod=" + workersPerPod);
      return false;
    }

    int podsToRemove = instancesToRemove / workersPerPod;

    if (podsToRemove > scalableSSReplicas) {
      LOG.severe(String.format("There are %d instances of scalable ComputeResource, "
          + "and %d instances requested to be removed", scalableSSReplicas, podsToRemove));
      return false;
    }

    boolean scaledDown =
        k8sController.patchStatefulSet(scalableSSName, scalableSSReplicas - podsToRemove);

    return scaledDown;
  }

  /**
   * send this message to all workers in the job
   * @param className
   * @param message
   * @return
   */
  @Override
  public boolean broadcastToAllWorkers(String className, Message message) {
    return false;
  }

  /**
   * generate the pod names that will be scaled up, newly created
   * @param instancesToAdd
   * @return
   */
  private  ArrayList<String> generatePodNames(int instancesToAdd) {

    ArrayList<String> podNames = new ArrayList<>();

    // this is the index of the first pod that will be created
    int podIndex = scalableSSReplicas;

    for (int i = 0; i < instancesToAdd; i++) {
      String podName = KubernetesUtils.podNameFromStatefulSetName(scalableSSName, podIndex + i);
      podNames.add(podName);
    }

    return podNames;
  }
}
