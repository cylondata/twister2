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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

import io.kubernetes.client.Exec;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
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
import okhttp3.OkHttpClient;

/**
 * a controller class to talk to the Kubernetes Master to manage jobs
 */

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private String namespace;
  private static ApiClient client = null;
  private static CoreV1Api coreApi;
  private static AppsV1Api appsApi;

  public void init(String nspace) {
    this.namespace = nspace;
    initApiInstances();
  }

  public static ApiClient getApiClient() {
    if (client != null) {
      return client;
    }
    try {
      client = ClientBuilder.standard()
          .setOverridePatchFormat(V1Patch.PATCH_FORMAT_JSON_PATCH)
          .build();
      return client;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }
  }

  public static void initApiInstances() {
    if (client == null) {
      getApiClient();
    }

    Configuration.setDefaultApiClient(client);
    coreApi = new CoreV1Api();
    appsApi = new AppsV1Api(client);
  }

  /**
   * create CoreV1Api that does not time out
   */
  public static CoreV1Api createCoreV1Api() {
    if (client == null) {
      getApiClient();
    }
    OkHttpClient httpClient =
        client.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    client.setHttpClient(httpClient);
    Configuration.setDefaultApiClient(client);
    return new CoreV1Api(client);
  }

  public static void close() {
    if (client != null && client.getHttpClient() != null
        && client.getHttpClient().connectionPool() != null) {

      client.getHttpClient().connectionPool().evictAll();
    }
  }

  /**
   * return the list of StatefulSet objects for this job,
   * otherwise return null
   */
  public List<V1StatefulSet> getJobStatefulSets(String jobID) {
    String labelSelector = KubernetesUtils.jobLabelSelector(jobID);
    try {
      V1StatefulSetList setList = appsApi.listNamespacedStatefulSet(
          namespace, null, null, null, null, labelSelector, null, null, null, null);
      return setList.getItems();
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting StatefulSet list.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * check whether given StatefulSet objects exist Kubernetes master,
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
   * return the list of worker StatefulSet names for this job
   * they must be in the form of "jobID-index"
   * otherwise return an empty ArrayList
   */
  public ArrayList<String> getJobWorkerStatefulSets(String jobID) {
    List<V1StatefulSet> ssList = getJobStatefulSets(jobID);
    ArrayList<String> ssNameList = new ArrayList<>();

    for (V1StatefulSet statefulSet : ssList) {
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
    try (okhttp3.Response response = appsApi.createNamespacedStatefulSetCall(
        namespace, statefulSet, null, null, null, null)
        .execute()) {

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

    Integer gracePeriodSeconds = 0;

    try (okhttp3.Response response = appsApi.deleteNamespacedStatefulSetCall(
        statefulSetName, namespace, null, null, gracePeriodSeconds, null,
        KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null)
        .execute()) {

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

    try (okhttp3.Response response = appsApi.patchNamespacedStatefulSetScaleCall(
        ssName, namespace, new V1Patch(jsonPatchStr), null, null, null, null, null)
        .execute()) {

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
    try (okhttp3.Response response = coreApi.createNamespacedServiceCall(
        namespace, service, null, null, null, null)
        .execute()) {

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
   * return the list of services that belong to this job
   * otherwise return an empty list
   */
  public List<V1Service> getJobServices(String jobID) {
    String labelSelector = KubernetesUtils.jobLabelSelector(jobID);
    try {
      V1ServiceList serviceList = coreApi.listNamespacedService(
          namespace, null, null, null, null, labelSelector, null, null, null, null);
      return serviceList.getItems();
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting service list.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * return existing service name if one of the services exist in Kubernetes master,
   * otherwise return null
   */
  public String existServices(List<String> serviceNames) {
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
        return service.getMetadata().getName();
      }
    }

    return null;
  }

  /**
   * delete the given service from Kubernetes master
   */
  public boolean deleteService(String serviceName) {

    Integer gracePeriodsSeconds = 0;

    try (okhttp3.Response response = coreApi.deleteNamespacedServiceCall(
        serviceName, namespace, null, null, gracePeriodsSeconds, null,
        KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY, null, null)
        .execute()) {

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
   * get PersistentVolumeClaim object for this job
   */
  public V1PersistentVolumeClaim getJobPersistentVolumeClaim(String jobID) {
    String labelSelector = KubernetesUtils.jobLabelSelector(jobID);
    try {
      V1PersistentVolumeClaimList pvcList = coreApi.listNamespacedPersistentVolumeClaim(
          namespace, null, null, null, null, labelSelector, null, null, null, null);

      if (pvcList.getItems().size() == 1) {
        return pvcList.getItems().get(0);
      } else if (pvcList.getItems().size() > 1) {
        throw new Twister2RuntimeException(
            "There are multiple PersistentVolumeClaim objects for this job on Kubernetes master");
      } else {
        return null;
      }

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting PersistentVolumeClaim list.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * create the given PersistentVolumeClaim on Kubernetes master
   */
  public boolean createPersistentVolumeClaim(V1PersistentVolumeClaim pvc) {

    String pvcName = pvc.getMetadata().getName();
    try (okhttp3.Response response = coreApi.createNamespacedPersistentVolumeClaimCall(
        namespace, pvc, null, null, null, null)
        .execute()) {

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

    try (okhttp3.Response response = coreApi.deleteNamespacedPersistentVolumeClaimCall(
        pvcName, namespace, null, null, null, null, null, null, null)
        .execute()) {

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "PersistentVolumeClaim [" + pvcName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.FINE, "There is no PersistentVolumeClaim [" + pvcName
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
    try (okhttp3.Response response = coreApi.createPersistentVolumeCall(pv, null, null, null, null)
        .execute()) {

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

    try (okhttp3.Response response = coreApi.deletePersistentVolumeCall(
        pvName, null, null, null, null, null, null, null)
        .execute()) {

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

  public static List<String> getUploaderWebServerPods(String ns, String uploaderLabel) {

    if (coreApi == null) {
      initApiInstances();
    }

    V1PodList podList = null;
    try {
      podList = coreApi.listNamespacedPod(
          ns, null, null, null, null, uploaderLabel, null, null, null, null);
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

  /**
   * create the given ConfigMap on Kubernetes master
   */
  public boolean createConfigMap(V1ConfigMap configMap) {

    String configMapName = configMap.getMetadata().getName();

    try (okhttp3.Response response = coreApi.createNamespacedConfigMapCall(
        namespace, configMap, null, null, null, null)
        .execute()) {

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "ConfigMap [" + configMapName + "] is created.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when creating the ConfigMap [" + configMapName
            + "]: Response: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the ConfigMap: " + configMapName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the ConfigMap: " + configMapName, e);
    }
    return false;
  }

  /**
   * delete the given ConfigMap from Kubernetes master
   */
  public boolean deleteConfigMap(String jobID) {

    String configMapName = jobID;
    int gracePeriodSeconds = 0;
    String propagationPolicy = KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY;

    try (okhttp3.Response response = coreApi.deleteNamespacedConfigMapCall(
        configMapName, namespace, null, null, gracePeriodSeconds, null, propagationPolicy,
        null, null).execute()) {

      if (response.isSuccessful()) {
        LOG.info("ConfigMap [" + configMapName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.warning("There is no ConfigMap [" + configMapName
              + "] to delete on Kubernetes master. It may have already been deleted.");
          return true;
        }

        LOG.severe("Error when deleting the ConfigMap [" + configMapName + "]: " + response);
        return false;
      }
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the ConfigMap: " + configMapName, e);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the ConfigMap: " + configMapName, e);
    }
    return false;
  }

  /**
   * return true if there is already a ConfigMap object with the same name on Kubernetes master,
   * otherwise return false
   */
  public boolean existConfigMap(String jobID) {
    String configMapName = jobID;
    V1ConfigMapList configMapList = null;
    try {
      configMapList = coreApi.listNamespacedConfigMap(namespace,
          null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting ConfigMap list.", e);
      throw new RuntimeException(e);
    }

    for (V1ConfigMap configMap : configMapList.getItems()) {
      if (configMapName.equals(configMap.getMetadata().getName())) {
        return true;
      }
    }

    return false;
  }

  /**
   * return the ConfigMap object of this job, if it exists,
   * otherwise return null
   */
  public V1ConfigMap getJobConfigMap(String jobID) {

    String labelSelector = KubernetesUtils.jobLabelSelector(jobID);
    try {
      V1ConfigMapList configMapList = coreApi.listNamespacedConfigMap(namespace,
          null, null, null, null, labelSelector, null, null, null, null);

      if (configMapList.getItems().size() == 1) {
        return configMapList.getItems().get(0);
      } else if (configMapList.getItems().size() > 1) {
        throw new Twister2RuntimeException(
            "There are multiple ConfigMaps for this job on Kubernetes master.");
      } else {
        return null;
      }

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting ConfigMap list.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * return configMap parameter value for the given jobID
   * if there is no value for the key, return null
   */
  public String getConfigMapParam(String jobID, String keyName) {
    Map<String, String> pairs = getConfigMapParams(jobID);
    return pairs.get(keyName);
  }

  /**
   * return all configMap parameters for the given jobID
   * if there is no ConfigMap, throw an exception
   */
  public Map<String, String> getConfigMapParams(String jobID) {
    String configMapName = KubernetesUtils.createConfigMapName(jobID);
    V1ConfigMap configMap = getJobConfigMap(jobID);
    if (configMap == null) {
      throw new RuntimeException("Could not get ConfigMap from K8s master: " + configMapName);
    }

    Map<String, String> pairs = configMap.getData();
    if (pairs == null) {
      throw new RuntimeException("Could not get data from ConfigMap");
    }

    return pairs;
  }

  /**
   * return restart count for the given workerID
   * if there is no key for the given workerID, return -1
   */
  public int getRestartCount(String jobID, String keyName) {
    String countStr = getConfigMapParam(jobID, keyName);
    if (countStr == null) {
      return -1;
    } else {
      return Integer.parseInt(countStr);
    }
  }

  /**
   * add a new parameter the job ConfigMap
   */
  public boolean addConfigMapParam(String jobID, String keyName, String value) {

    String configMapName = KubernetesUtils.createConfigMapName(jobID);
    String valueStr = "\"" + value + "\"";

    String jsonPatchStr =
        "[{\"op\":\"add\",\"path\":\"/data/" + keyName + "\",\"value\":" + valueStr + "}]";

    try (okhttp3.Response response = coreApi.patchNamespacedConfigMapCall(
        configMapName, namespace, new V1Patch(jsonPatchStr), null, null, null, null, null)
        .execute()) {

      if (response.isSuccessful()) {
        LOG.fine("ConfigMap parameter added " + keyName + " = " + value);
        return true;

      } else {
        LOG.severe("Error when patching the ConfigMap [" + configMapName + "]: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when patching the ConfigMap: " + configMapName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when patching the StatefulSet: " + configMapName, e);
    }
    return false;
  }

  /**
   * update a parameter value in the job ConfigMap
   */
  public boolean updateConfigMapParam(String jobID, String paramName, String paramValue) {

    String configMapName = KubernetesUtils.createConfigMapName(jobID);
    String countStr = "\"" + paramValue + "\"";

    String jsonPatchStr =
        "[{\"op\":\"replace\",\"path\":\"/data/" + paramName + "\",\"value\":" + countStr + "}]";

    try (okhttp3.Response response = coreApi.patchNamespacedConfigMapCall(
        configMapName, namespace, new V1Patch(jsonPatchStr), null, null, null, null, null)
        .execute()) {

      if (response.isSuccessful()) {
        LOG.fine("ConfigMap parameter updated " + paramName + " = " + paramValue);
        return true;

      } else {
        LOG.severe("Error when patching the ConfigMap [" + configMapName + "]: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when patching the ConfigMap: " + configMapName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when patching the ConfigMap: " + configMapName, e);
    }
    return false;
  }

  public boolean updateConfigMapJobParam(JobAPI.Job job) {
    String jobAsEncodedStr = Base64.getEncoder().encodeToString(job.toByteArray());
    return updateConfigMapParam(
        job.getJobId(), KubernetesConstants.JOB_OBJECT_CM_PARAM, jobAsEncodedStr);
  }

  /**
   * remove a restart count from the job ConfigMap
   * this is used when the job is scaled down
   */
  public boolean removeRestartCount(String jobID, String keyName) {

    String configMapName = KubernetesUtils.createConfigMapName(jobID);

    String jsonPatchStr =
        "[{\"op\":\"remove\",\"path\":\"/data/" + keyName + "\"}]";

    try (okhttp3.Response response = coreApi.patchNamespacedConfigMapCall(
        configMapName, namespace, new V1Patch(jsonPatchStr), null, null, null, null, null)
        .execute()) {

      if (response.isSuccessful()) {
        LOG.fine("ConfigMap parameter removed " + keyName);
        return true;

      } else {
        LOG.severe("Error removing restartCount from the ConfigMap ["
            + configMapName + "]: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when patching the ConfigMap: " + configMapName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when patching the ConfigMap: " + configMapName, e);
    }
    return false;
  }

}
