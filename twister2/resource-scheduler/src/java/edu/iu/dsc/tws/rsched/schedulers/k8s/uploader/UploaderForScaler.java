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
package edu.iu.dsc.tws.rsched.schedulers.k8s.uploader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodCondition;
import io.kubernetes.client.util.Watch;

public class UploaderForScaler extends Thread {
  private static final Logger LOG = Logger.getLogger(UploaderForScaler.class.getName());

  private CoreV1Api coreApi;
  private ApiClient apiClient;

  private String namespace;
  private String jobName;
  private String ssName;

  public UploaderForScaler(String namespace, String jobName, String ssName) {
    this.namespace = namespace;
    this.jobName = jobName;
    this.ssName = ssName;
  }

  @Override
  public void run() {
    createApiInstances();
    watchScaledUpPods();
  }

  private void createApiInstances() {

    try {
      apiClient = io.kubernetes.client.util.Config.defaultClient();
      apiClient.getHttpClient().setReadTimeout(0, TimeUnit.MILLISECONDS);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }
    Configuration.setDefaultApiClient(apiClient);

    coreApi = new CoreV1Api(apiClient);
  }

  private void watchScaledUpPods() {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String workerRoleLabel = KubernetesUtils.createWorkerRoleLabelWithKey(jobName);

    Integer timeoutSeconds = Integer.MAX_VALUE;
    Watch<V1Pod> watch = null;

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, workerRoleLabel,
              null, null, timeoutSeconds, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the pods to get the IPs: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    int eventCount = 0;
    for (Watch.Response<V1Pod> item : watch) {

      if (item.object != null
          && item.object.getMetadata().getName().startsWith(ssName)
      //    && phase.equals(item.object.getStatus().getPhase())
      ) {
        String podName = item.object.getMetadata().getName();
        String podPhase = item.object.getStatus().getPhase();
        List<V1PodCondition> conditions = item.object.getStatus().getConditions();
        String condCount = "conditions: " + conditions.size();
        String type = "";
        if (conditions.size() != 0) {
          type = "cond type: ";
          for (V1PodCondition condition: conditions) {
            type += condition.getType() + ", ";
          }
        }

        LOG.info(eventCount++ + " -------------- Pod event: " + podName + ", " + podPhase + ", "
            + condCount + ", " + type);
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

  }

}
