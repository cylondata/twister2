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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimList;
import io.kubernetes.client.openapi.models.V1PersistentVolumeList;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetList;
import io.kubernetes.client.util.ClientBuilder;

/**
 * a controller class to talk to the Kubernetes Master to manage jobs
 */

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private String namespace;
  private ApiClient client = null;
  private CoreV1Api coreApi;
  private AppsV1Api appsApi;

  public void init(String nspace) {
    this.namespace = nspace;
    createApiInstances();
  }

  public void createApiInstances() {
    try {
      client = io.kubernetes.client.util.Config.defaultClient();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }
    Configuration.setDefaultApiClient(client);

    coreApi = new CoreV1Api();
    appsApi = new AppsV1Api(client);
  }

  /**
   * return the StatefulSet object if it exists in the Kubernetes master,
   * otherwise return null
   */
  public boolean existStatefulSets(List<String> statefulSetNames) {
    V1StatefulSetList setList = null;
    try {
      setList = appsApi.listNamespacedStatefulSet(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting StatefulSet list.", e);
      throw new RuntimeException(e);
    }

    for (V1StatefulSet statefulSet : setList.getItems()) {
      if (statefulSetNames.contains(statefulSet.getMetadata().getName())) {
        LOG.severe("There is already a StatefulSet with the name: "
            + statefulSet.getMetadata().getName());
        return true;
      }
    }

    return false;
  }

  /**
   * return the list of StatefulSet names that matches this jobs StatefulSet names for workers
   * they must be in the form of "jobID-index"
   * otherwise return an empty ArrayList
   */
  public ArrayList<String> getStatefulSetsForJobWorkers(String jobID) {
    V1StatefulSetList setList = null;
    try {
      setList = appsApi.listNamespacedStatefulSet(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting StatefulSet list.", e);
      throw new RuntimeException(e);
    }

    ArrayList<String> ssNameList = new ArrayList<>();

    for (V1StatefulSet statefulSet : setList.getItems()) {
      String ssName = statefulSet.getMetadata().getName();
      if (ssName.matches(jobID + "-" + "[0-9]+")) {
        ssNameList.add(ssName);
      }
    }

    return ssNameList;
  }

  /**
   * create the given StatefulSet on Kubernetes master
   */
  public boolean createStatefulSet(V1StatefulSet statefulSet) {

    String statefulSetName = statefulSet.getMetadata().getName();
    try {
      okhttp3.Response response = appsApi.createNamespacedStatefulSetCall(
          namespace, statefulSet, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet [" + statefulSetName + "] is created.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when creating the StatefulSet [" + statefulSetName + "]: "
            + response);
        LOG.log(Level.SEVERE, "Submitted StatefulSet Object: " + statefulSet);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the StatefulSet: " + statefulSetName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the StatefulSet: " + statefulSetName, e);
    }
    return false;
  }

  /**
   * delete the given StatefulSet from Kubernetes master
   */
  public boolean deleteStatefulSet(String statefulSetName) {

    try {
      Integer gracePeriodSeconds = 0;

      okhttp3.Response response = appsApi.deleteNamespacedStatefulSetCall(
          statefulSetName, namespace, null, null, gracePeriodSeconds, null,
          KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet [" + statefulSetName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.SEVERE, "There is no StatefulSet [" + statefulSetName
              + "] to delete on Kubernetes master. It may have already terminated.");
          return true;
        }

        LOG.log(Level.SEVERE, "Error when deleting the StatefulSet ["
            + statefulSetName + "]: " + response);
        return false;
      }

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the StatefulSet: " + statefulSetName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the StatefulSet: " + statefulSetName, e);
      return false;
    }
  }

  /**
   * scale up or down the given StatefulSet
   */
  public boolean patchStatefulSet(String ssName, int replicas) {

    String jsonPatchStr =
        "[{\"op\":\"replace\",\"path\":\"/spec/replicas\",\"value\":" + replicas + "}]";

    // json-patch a deployment
    ApiClient jsonPatchClient;
    try {
      jsonPatchClient =
          ClientBuilder.standard().setOverridePatchFormat(V1Patch.PATCH_FORMAT_JSON_PATCH).build();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error when creating patch client: " + ssName, e);
      return false;
    }

    AppsV1Api patchApi = new AppsV1Api(jsonPatchClient);

    try {
      okhttp3.Response response = patchApi.patchNamespacedStatefulSetScaleCall(
          ssName, namespace, new V1Patch(jsonPatchStr), null, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet [" + ssName + "] is patched.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when patching the StatefulSet [" + ssName + "]: "
            + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when patching the StatefulSet: " + ssName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when patching the StatefulSet: " + ssName, e);
    }
    return false;
  }

  /**
   * create the given service on Kubernetes master
   */
  public boolean createService(V1Service service) {

    String serviceName = service.getMetadata().getName();
    try {
      okhttp3.Response response = coreApi.createNamespacedServiceCall(
          namespace, service, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "Service [" + serviceName + "] created.");
        return true;
      } else {
        LOG.log(Level.SEVERE, "Error when creating the service [" + serviceName + "]: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the service: " + serviceName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the service: " + serviceName, e);
    }
    return false;
  }

  /**
   * return true if one of the services exist in Kubernetes master,
   * otherwise return false
   */
  public boolean existServices(List<String> serviceNames) {
// sending the request with label does not work for list services call
//    String label = "app=" + serviceLabel;
    V1ServiceList serviceList = null;
    try {
      serviceList = coreApi.listNamespacedService(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting service list.", e);
      throw new RuntimeException(e);
    }

    for (V1Service service : serviceList.getItems()) {
      if (serviceNames.contains(service.getMetadata().getName())) {
        LOG.severe("There is already a service with the name: " + service.getMetadata().getName());
        return true;
      }
    }

    return false;
  }

  /**
   * delete the given service from Kubernetes master
   */
  public boolean deleteService(String serviceName) {

    Integer gracePeriodsSeconds = 0;

    try {
      okhttp3.Response response = coreApi.deleteNamespacedServiceCall(
          serviceName, namespace, null, null, gracePeriodsSeconds, null,
          KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.info("Service [" + serviceName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.warning("There is no Service [" + serviceName
              + "] to delete on Kubernetes master. It may have already been terminated.");
          return true;
        }

        LOG.severe("Error when deleting the Service [" + serviceName + "]: " + response);
        return false;
      }
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the service: " + serviceName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the service: " + serviceName, e);
      return false;
    }
  }

  /**
   * get the IP address of the given service
   * otherwise return null
   */
  public String getServiceIP(String serviceName) {
    V1ServiceList serviceList = null;
    try {
      serviceList = coreApi.listNamespacedService(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting service list.", e);
      throw new RuntimeException(e);
    }

    for (V1Service service : serviceList.getItems()) {
      if (serviceName.equals(service.getMetadata().getName())) {
        return service.getSpec().getClusterIP();
      }
    }

    return null;
  }

  /**
   * sending a command to shell
   */
  public static boolean runProcess(String[] command) {
    StringBuilder stderr = new StringBuilder();
    int status =
        ProcessUtils.runSyncProcess(false, command, stderr, new File("."), false);

//    if (status != 0) {
//      LOG.severe(String.format(
//          "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s", command, stdout, stderr));
//    }
    return status == 0;
  }

  /**
   * check whether the given PersistentVolumeClaim exist on Kubernetes master
   */
  public boolean existPersistentVolumeClaim(String pvcName) {
    V1PersistentVolumeClaimList pvcList = null;
    try {
      pvcList = coreApi.listNamespacedPersistentVolumeClaim(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting PersistentVolumeClaim list.", e);
      throw new RuntimeException(e);
    }

    for (V1PersistentVolumeClaim pvc : pvcList.getItems()) {
      if (pvcName.equals(pvc.getMetadata().getName())) {
        LOG.severe("There is already a PersistentVolumeClaim with the name: " + pvcName);
        return true;
      }
    }

    return false;
  }

  /**
   * create the given PersistentVolumeClaim on Kubernetes master
   */
  public boolean createPersistentVolumeClaim(V1PersistentVolumeClaim pvc) {

    String pvcName = pvc.getMetadata().getName();
    try {
      okhttp3.Response response = coreApi.createNamespacedPersistentVolumeClaimCall(
          namespace, pvc, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "PersistentVolumeClaim [" + pvcName + "] is created.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when creating the PersistentVolumeClaim [" + pvcName
            + "] Response: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the PersistentVolumeClaim: " + pvcName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the PersistentVolumeClaim: " + pvcName, e);
    }
    return false;
  }

  public boolean deletePersistentVolumeClaim(String pvcName) {

    try {
      okhttp3.Response response = coreApi.deleteNamespacedPersistentVolumeClaimCall(
          pvcName, namespace, null, null, null, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "PersistentVolumeClaim [" + pvcName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.WARNING, "There is no PersistentVolumeClaim [" + pvcName
              + "] to delete on Kubernetes master. It may have already been deleted.");
          return true;
        }

        LOG.log(Level.SEVERE, "Error when deleting the PersistentVolumeClaim [" + pvcName
            + "] Response: " + response);
        return false;
      }
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the PersistentVolumeClaim: " + pvcName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the PersistentVolumeClaim: " + pvcName, e);
      return false;
    }
  }

  public V1PersistentVolume getPersistentVolume(String pvName) {
    V1PersistentVolumeList pvList = null;
    try {
      pvList = coreApi.listPersistentVolume(null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting PersistentVolume list.", e);
      throw new RuntimeException(e);
    }

    for (V1PersistentVolume pv : pvList.getItems()) {
      if (pvName.equals(pv.getMetadata().getName())) {
        return pv;
      }
    }

    return null;
  }

  /**
   * create the given service on Kubernetes master
   */
  public boolean createPersistentVolume(V1PersistentVolume pv) {

    String pvName = pv.getMetadata().getName();
    try {
      okhttp3.Response response = coreApi.createPersistentVolumeCall(pv, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "PersistentVolume [" + pvName + "] is created.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when creating the PersistentVolume [" + pvName + "]: "
            + response);
//        LOG.log(Level.SEVERE, "Submitted PersistentVolume Object: " + pv);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the PersistentVolume: " + pvName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the PersistentVolume: " + pvName, e);
    }
    return false;
  }


  public boolean deletePersistentVolume(String pvName) {

    try {
      okhttp3.Response response = coreApi.deletePersistentVolumeCall(
          pvName, null, null, null, null, null, null, null)
          .execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "PersistentVolume [" + pvName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.WARNING, "There is no PersistentVolume [" + pvName
              + "] to delete on Kubernetes master. It may have already been deleted.");
          return true;
        }

        LOG.log(Level.SEVERE, "Error when deleting the PersistentVolume [" + pvName + "]: "
            + response);
        return false;
      }
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the PersistentVolume: " + pvName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the PersistentVolume: " + pvName, e);
      return false;
    }
  }

  /**
   * return true if the Secret object with that name exists in Kubernetes master,
   * otherwise return false
   */
  public boolean existSecret(String secretName) {
    V1SecretList secretList = null;
    try {
      secretList = coreApi.listNamespacedSecret(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting Secret list.", e);
      throw new RuntimeException(e);
    }

    for (V1Secret secret : secretList.getItems()) {
      if (secretName.equalsIgnoreCase(secret.getMetadata().getName())) {
        return true;
      }
    }

    return false;
  }

  /**
   * get NodeInfoUtils objects for the nodes on this cluster
   *
   * @return the NodeInfoUtils object list. If it can not get the list from K8s master, return null.
   */
  public ArrayList<JobMasterAPI.NodeInfo> getNodeInfo(String rackLabelKey,
                                                      String datacenterLabelKey) {

    V1NodeList nodeList = null;
    try {
      nodeList = coreApi.listNode(null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting NodeList.", e);
      return null;
    }

    ArrayList<JobMasterAPI.NodeInfo> nodeInfoList = new ArrayList<>();
    for (V1Node node : nodeList.getItems()) {
      List<V1NodeAddress> addressList = node.getStatus().getAddresses();
      for (V1NodeAddress nodeAddress : addressList) {
        if ("InternalIP".equalsIgnoreCase(nodeAddress.getType())) {
          String nodeIP = nodeAddress.getAddress();
          String rackName = null;
          String datacenterName = null;

          // get labels
          Map<String, String> labelMap = node.getMetadata().getLabels();
          for (String key : labelMap.keySet()) {
            if (key.equalsIgnoreCase(rackLabelKey)) {
              rackName = labelMap.get(key);
            }
            if (key.equalsIgnoreCase(datacenterLabelKey)) {
              datacenterName = labelMap.get(key);
            }
          }

          JobMasterAPI.NodeInfo nodeInfo =
              NodeInfoUtils.createNodeInfo(nodeIP, rackName, datacenterName);
          nodeInfoList.add(nodeInfo);
          break;
        }
      }
    }

    return nodeInfoList;
  }

  public List<String> getUploaderWebServerPods(String uploaderLabel) {

    V1PodList podList = null;
    try {
      podList = coreApi.listNamespacedPod(
          namespace, null, null, null, null, uploaderLabel, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting uploader pod list.", e);
      throw new RuntimeException(e);
    }

    List<String> podNames = podList
        .getItems()
        .stream()
        .map(v1pod -> v1pod.getMetadata().getName())
        .collect(Collectors.toList());

    return podNames;
  }

  /**
   * delete job package file from uploader web server pods
   * currently it does not delete job package file from multiple uploader pods
   * so, It is not in use
   */
  public boolean deleteJobPackage(List<String> uploaderPods, String jobPackageName) {

    // command to execute
    // if [ -f test.txt ]; then rm -f test.txt; else exit 1; fi
    // if file exist, remove it. Otherwise exit 1
//    String command = String.format("if [ -f %s ]; then rm -f %s; else exit 1; fi",
//        jobPackageName, jobPackageName);
    String command = String.format("rm -f %s", jobPackageName);
    String[] fullCommand = {"bash", "-c", command};

    boolean allDeleted = true;
    for (String uploaderPod : uploaderPods) {

      try {
        Exec exec = new Exec(client);
        final Process proc = exec.exec(namespace, uploaderPod, fullCommand, false, false);
        proc.waitFor();
        proc.destroy();

        if (proc.exitValue() == 0) {
          LOG.info("Deleted job package from uploader web server pod: " + uploaderPod);
        } else {
          LOG.info("Could not delete the job package from uploader web server pod: " + uploaderPod
              + ", process exit code: " + proc.exitValue());
          allDeleted = false;
        }

      } catch (ApiException e) {
        LOG.log(Level.INFO,
            String.format("Exception when deleting the job package from uploader web server [%s]",
                uploaderPod), e);
      } catch (IOException e) {
        LOG.log(Level.INFO,
            String.format("Exception when deleting the job package from uploader web server [%s]",
                uploaderPod), e);
      } catch (InterruptedException e) {
        LOG.log(Level.INFO,
            String.format("Exception when deleting the job package from uploader web server [%s]",
                uploaderPod), e);
      }
    }

    return allDeleted;
  }

}
