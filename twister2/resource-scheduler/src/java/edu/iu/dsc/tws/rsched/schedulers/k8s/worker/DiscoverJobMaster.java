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
package edu.iu.dsc.tws.rsched.schedulers.k8s.worker;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Watch;

public class DiscoverJobMaster {
  private static final Logger LOG = Logger.getLogger(DiscoverJobMaster.class.getName());

  private static CoreV1Api coreApi;
  private static ApiClient apiClient;

  public DiscoverJobMaster() {
    createApiInstances();
  }

  public static void createApiInstances() {
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

  public static CoreV1Api getCoreApi() {
    if (coreApi == null) {
      createApiInstances();
    }

    return coreApi;
  }

  /**
   * watch events until getting the Running event for job master pod
   */
  public String waitUntilJobMasterRunning(String jobMasterPodName, long timeoutMiliSec) {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String phase = "Running";
    String namespace = "default";
//    String servicelabel = KubernetesUtils.createServiceLabelWithKey(jobName);
    Integer timeoutSeconds = (int) (timeoutMiliSec / 1000);
    Watch<V1Pod> watch = null;

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, null,
              null, null, timeoutSeconds, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the pods for the job: " + "\n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    String podIP = null;

    for (Watch.Response<V1Pod> item : watch) {
      if (item.object != null
          && jobMasterPodName.equals(item.object.getMetadata().getName())
          && phase.equals(item.object.getStatus().getPhase())) {

        LOG.log(Level.INFO, "Received pod Running event for the job master pod: "
            + item.object.getMetadata().getName());

        podIP = item.object.getStatus().getPodIP();
        break;
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

    return podIP;
  }


}
