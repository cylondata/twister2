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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class KubernetesUtils {
  private static final Logger LOG = Logger.getLogger(KubernetesUtils.class.getName());

  private KubernetesUtils() {
  }

  /**
   * when the given name is in the form of "name-index"
   * it returns the index as int
   * @param name
   * @return
   */
  public static int indexFromName(String name) {
    return Integer.parseInt(name.substring(name.lastIndexOf("-") + 1));
  }

  /**
   * when the given name is in the form of "name-index"
   * it returns the name by removing the dash and the index
   * @param name
   * @return
   */
  public static String removeIndexFromName(String name) {
    return name.substring(0, name.lastIndexOf("-"));
  }

  /**
   * create file copy command to a pod
   * @return
   */
  public static String[] createCopyCommand(String filename,
                                           String namespace,
                                           String podName,
                                           String podFile) {

    String targetDir = String.format("%s/%s:%s", namespace, podName, podFile);
    return new String[]{"kubectl", "cp", filename, targetDir};
  }

  /**
   * create podName from StatefulSet name with pod index
   * @return
   */
  public static String podNameFromStatefulSetName(String ssName, int podIndex) {
    return ssName + "-" + podIndex;
  }

  /**
   * create service name from job name
   * @param jobID
   * @return
   */
  public static String createServiceName(String jobID) {
    return KubernetesConstants.TWISTER2_SERVICE_PREFIX + jobID;
  }

  /**
   * create service name from job name
   * @param jobID
   * @return
   */
  public static String createJobMasterServiceName(String jobID) {
    return KubernetesConstants.TWISTER2_SERVICE_PREFIX + jobID + "-jm";
  }

  /**
   * create persistent volume claim name name from the job name
   * @param jobID
   * @return
   */
  public static String createPersistentVolumeClaimName(String jobID) {
    return KubernetesConstants.TWISTER2_STORAGE_CLAIM_PREFIX + jobID;
  }

  /**
   * create storage claim name name from job name
   * @param jobID
   * @return
   */
  public static String createPersistentVolumeName(String jobID) {
    return "persistent-volume-" + jobID;
  }

  /**
   * create service label from job name
   * this label is used when constructing statefulset
   * @param jobID
   * @return
   */
  public static String createServiceLabel(String jobID) {
    return KubernetesConstants.SERVICE_LABEL_PREFIX + jobID;
  }

  /**
   * create service label from job name
   * this label is used when constructing statefulset
   * @param jobID
   * @return
   */
  public static String createJobMasterServiceLabel(String jobID) {
    return KubernetesConstants.SERVICE_LABEL_PREFIX + jobID + "-jm";
  }

  public static String createJobMasterRoleLabel(String jobID) {
    return jobID + "-jm";
  }

  public static String createWorkerRoleLabel(String jobID) {
    return jobID + "-worker";
  }

  public static String createJobPodsLabel(String jobID) {
    return KubernetesConstants.TWISTER2_JOB_PODS_LABEL_PREFIX + jobID;
  }

  /**
   * this label is used when submitting queries to kubernetes master
   * @param jobID
   * @return
   */
  public static String createServiceLabelWithKey(String jobID) {
    return KubernetesConstants.SERVICE_LABEL_KEY + "=" + createServiceLabel(jobID);
  }

  /**
   * this label is used when submitting queries to kubernetes master
   * @param jobID
   * @return
   */
  public static String createJobMasterServiceLabelWithKey(String jobID) {
    return KubernetesConstants.SERVICE_LABEL_KEY + "=" + createJobMasterServiceLabel(jobID);
  }

  public static String createJobPodsLabelWithKey(String jobID) {
    return KubernetesConstants.TWISTER2_JOB_PODS_KEY + "=" + createJobPodsLabel(jobID);
  }

  public static String createJobMasterRoleLabelWithKey(String jobID) {
    return KubernetesConstants.TWISTER2_PODS_ROLE_KEY + "=" + createJobMasterRoleLabel(jobID);
  }

  public static String createWorkerRoleLabelWithKey(String jobID) {
    return KubernetesConstants.TWISTER2_PODS_ROLE_KEY + "=" + createWorkerRoleLabel(jobID);
  }

  /**
   * create container name with the given containerIndex
   * each container in a pod will have a unique name with this index
   * @param containerIndex
   * @return
   */
  public static String createContainerName(int containerIndex) {
    return KubernetesConstants.CONTAINER_NAME_PREFIX + containerIndex;
  }

  /**
   * create StatefulSet name for workers
   * add the given index a suffix to the job name
   * @return
   */
  public static String createWorkersStatefulSetName(String jobID, int index) {
    return jobID + "-" + index;
  }

  /**
   * create StatefulSet name for the given job name
   * add a suffix to job name
   * @return
   */
  public static String createJobMasterStatefulSetName(String jobID) {
    return jobID + "-jm";
  }

  /**
   * create pod name for the job master
   * there will be one pod for the job master
   * we add a suffix to statefulset name
   * @return
   */
  public static String createJobMasterPodName(String jobID) {
    return createJobMasterStatefulSetName(jobID) + "-0";
  }

  /**
   * create ConfigMap name for the given job name
   * add a suffix to job name
   * @return
   */
  public static String createConfigMapName(String jobID) {
    return jobID + "-cm";
  }

  /**
   * create the key for worker restart count to be used in ConfigMap
   * @return
   */
  public static String createRestartWorkerKey(int workerID) {
    return "RESTART_COUNT_FOR_WORKER_" + workerID;
  }

  /**
   * create the key for job master restart count to be used in ConfigMap
   * @return
   */
  public static String createRestartJobMasterKey() {
    return "RESTART_COUNT_FOR_JOB_MASTER";
  }

  public static String jobPackageFullPath(Config config, String jobID) {
    String uploaderDir = KubernetesContext.uploaderWebServerDirectory(config);
    String jobPackageFullPath = uploaderDir + "/" + JobUtils.createJobPackageFileName(jobID);
    return jobPackageFullPath;
  }

  public static String getLocalAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
      return null;
    }
  }

  public static InetAddress convertToIPAddress(String ipStr) {
    try {
      return InetAddress.getByName(ipStr);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Exception when converting to IP adress: ", e);
      return null;
    }
  }

  /**
   * calculate the number of pods in a job
   * @param job
   * @return
   */
  public static int numberOfWorkerPods(JobAPI.Job job) {

    int podsCount = 0;

    for (JobAPI.ComputeResource computeResource: job.getComputeResourceList()) {
      podsCount += computeResource.getInstances();
    }

    return podsCount;
  }

  /**
   * generate all pod names in a job
   * @param job
   * @return
   */
  public static ArrayList<String> generatePodNames(JobAPI.Job job) {

    ArrayList<String> podNames = new ArrayList<>();
    List<JobAPI.ComputeResource> resourceList = job.getComputeResourceList();

    for (int i = 0; i < resourceList.size(); i++) {

      JobAPI.ComputeResource computeResource = resourceList.get(i);
      int podsCount = computeResource.getInstances();
      int index = computeResource.getIndex();

      for (int j = 0; j < podsCount; j++) {
        String ssName = KubernetesUtils.createWorkersStatefulSetName(job.getJobId(), index);
        String podName = KubernetesUtils.podNameFromStatefulSetName(ssName, j);
        podNames.add(podName);
      }
    }

    return podNames;
  }
}
