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
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class KubernetesUtils {
  private static final Logger LOG = Logger.getLogger(KubernetesUtils.class.getName());

  private KubernetesUtils() {
  }

  /**
   * when the given name is in the form of "name-id"
   * it returns the id as int
   * @param name
   * @return
   */
  public static int idFromName(String name) {
    return Integer.parseInt(name.substring(name.lastIndexOf("-") + 1));
  }

  /**
   * create file copy command to a pod
   * @return
   */
  public static String[] createCopyCommand(String filename, String namespace, String podName) {

    String targetDir = String.format("%s/%s:%s", namespace, podName, POD_MEMORY_VOLUME);
    return new String[]{"kubectl", "cp", filename, targetDir};
  }

  /**
   * create podName from jobName with pod index
   * @param jobName
   * @return
   */
  public static String podNameFromJobName(String jobName, int podIndex) {
    return jobName + "-" + podIndex;
  }

  /**
   * create service name from job name
   * @param jobName
   * @return
   */
  public static String createServiceName(String jobName) {
    return KubernetesConstants.TWISTER2_SERVICE_PREFIX + jobName;
  }

  /**
   * create service name from job name
   * @param jobName
   * @return
   */
  public static String createJobMasterServiceName(String jobName) {
    return KubernetesConstants.TWISTER2_SERVICE_PREFIX + jobName + "-job-master";
  }

  /**
   * create persistent volume claim name name from the job name
   * @param jobName
   * @return
   */
  public static String createPersistentVolumeClaimName(String jobName) {
    return KubernetesConstants.TWISTER2_STORAGE_CLAIM_PREFIX + jobName;
  }

  /**
   * create storage claim name name from job name
   * @param jobName
   * @return
   */
  public static String createPersistentVolumeName(String jobName) {
    return "persistent-volume-" + jobName;
  }

  /**
   * create service label from job name
   * this label is used when constructing statefulset
   * @param jobName
   * @return
   */
  public static String createServiceLabel(String jobName) {
    return KubernetesConstants.SERVICE_LABEL_PREFIX + jobName;
  }

  /**
   * create service label from job name
   * this label is used when constructing statefulset
   * @param jobName
   * @return
   */
  public static String createJobMasterServiceLabel(String jobName) {
    return KubernetesConstants.SERVICE_LABEL_PREFIX + jobName + "-job-master";
  }

  public static String createJobPodsLabel(String jobName) {
    return KubernetesConstants.TWISTER2_JOB_PODS_PREFIX + jobName;
  }

  /**
   * this label is used when submitting queries to kubernetes master
   * @param jobName
   * @return
   */
  public static String createServiceLabelWithKey(String jobName) {
    return KubernetesConstants.SERVICE_LABEL_KEY + "=" + createServiceLabel(jobName);
  }

  /**
   * this label is used when submitting queries to kubernetes master
   * @param jobName
   * @return
   */
  public static String createJobMasterServiceLabelWithKey(String jobName) {
    return KubernetesConstants.SERVICE_LABEL_KEY + "=" + createJobMasterServiceLabel(jobName);
  }

  public static String createJobPodsLabelWithKey(String jobName) {
    return KubernetesConstants.TWISTER2_JOB_PODS_KEY + "=" + createJobPodsLabel(jobName);
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
   * create StatefulSet name for the given job name
   * add a suffix to job name
   * @return
   */
  public static String createJobMasterStatefulSetName(String jobName) {
    return jobName + "-job-master";
  }

  /**
   * create pod name for the job master
   * there will be one pod for the job master
   * we add a suffix to statefulset name
   * @return
   */
  public static String createJobMasterPodName(String jobName) {
    return createJobMasterStatefulSetName(jobName) + "-0";
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

}
