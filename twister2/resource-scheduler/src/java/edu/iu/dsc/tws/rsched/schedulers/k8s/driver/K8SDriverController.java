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

import java.util.ArrayList;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriverController;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.driver.JMDriverClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.JobPackageTransferThread;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

public class K8SDriverController implements IDriverController {

  private static final Logger LOG = Logger.getLogger(K8SDriverController.class.getName());

  private JMDriverClient driverClient;
  private JobAPI.Job job;
  private KubernetesController k8sController;
  private Config config;

  // replicas and workersPerPod values for scalable compute resource (scalable statefulSet)
  private String scalableSSName;
  private int replicas;
  private int workersPerPod;
  private int computeResourceIndex;

  // job package file to be transferred to newly created pods
  private String jobPackageFile;

  public K8SDriverController(Config config, String jmHost, JobAPI.Job job, String jobPackageFile,
                             KubernetesController k8sController) {
    this.config = config;
    this.job = job;
    this.jobPackageFile = jobPackageFile;
    this.k8sController = k8sController;

    computeResourceIndex = job.getComputeResourceCount() - 1;
    this.replicas = job.getComputeResource(computeResourceIndex).getInstances();
    this.workersPerPod = job.getComputeResource(computeResourceIndex).getWorkersPerPod();

    scalableSSName = KubernetesUtils.createWorkersStatefulSetName(
        job.getJobName(), job.getComputeResourceCount() - 1);

    int jmPort = JobMasterContext.jobMasterPort(config);
    driverClient = new JMDriverClient(config, jmHost, jmPort);
    driverClient.startThreaded();
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
        k8sController.patchStatefulSet(scalableSSName, replicas + podsToAdd);
    if (!scaledUp) {
      return false;
    }

    // complete the uploading
    if (KubernetesContext.clientToPodsUploading(config)) {
      boolean uploaded = JobPackageTransferThread.completeFileTransfers();

      // if scaling up pods is successful but uploading is unsuccessful,
      // scale down again
      if (!uploaded) {
        k8sController.patchStatefulSet(scalableSSName, replicas);
        return false;
      }
    }

    replicas = replicas + podsToAdd;

    driverClient.sendScaledMessage(computeResourceIndex, replicas);
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

    // send scaled message to job master
    return driverClient.sendScaledMessage(computeResourceIndex, replicas);
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
    int podIndex = replicas;

    for (int i = 0; i < instancesToAdd; i++) {
      String podName = KubernetesUtils.podNameFromStatefulSetName(scalableSSName, podIndex + i);
      podNames.add(podName);
    }

    return podNames;
  }

  /**
   * close the connection to the
   */
  public void close() {
    driverClient.close();
  }

}
