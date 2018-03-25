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

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.util.Watch;

/**
 * a class to watch events related to pods starting containers in them
 * this threads starts watching events for all pods in a job
 * when a container in a pod becomes Started, it marks that pod as ready
 * when all pods become ready, this thread stops watching events and finishes execution
 */

public class PodWatcher extends Thread {
  private static final Logger LOG = Logger.getLogger(PodWatcher.class.getName());

  private HashMap<String, Boolean> pods;
  private ApiClient client = null;
  private CoreV1Api coreApi;
  private String namespace;

  public PodWatcher(String namespace, String jobName, int numberOfPods,
                    ApiClient client, CoreV1Api coreApi) {

    this.namespace = namespace;
    this.client = client;
    this.coreApi = coreApi;

    pods = new HashMap<String, Boolean>();
    for (int i = 0; i < numberOfPods; i++) {
      String podName = KubernetesUtils.podNameFromJobName(jobName, i);
      pods.put(podName, false);
    }
  }

  /**
   * the list of watched pods
   * @return
   */
  public HashMap<String, Boolean> getPods() {
    return pods;
  }

  /**
   * watch for a Container Started event in all pods
   * then stop the execution
   */
  public void run() {
    /** Event Reasons: SuccessfulMountVolume, Killing, Scheduled, Pulled, Created, Started
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String reason = "Started";
    Watch<V1Event> watch = null;
    try {
      watch = Watch.createWatch(
          client,
          coreApi.listNamespacedEventCall(
              namespace, null, null, null, null, null, 10, null, null, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Event>>() { }.getType());

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Can not start event watcher for the namespace: " + namespace, e);
    }

    boolean result = false;
    int counter = 0;

    for (Watch.Response<V1Event> item : watch) {
      if (item.object != null && reason.equals(item.object.getReason())) {

        String involvedPod = item.object.getInvolvedObject().getName();
        if (pods.containsKey(involvedPod) && !pods.get(involvedPod)) {
          pods.put(involvedPod, true);
          LOG.log(Level.INFO, "Container started event received for the pod: " + involvedPod);

          counter++;
          if (counter == pods.size()) {
            LOG.log(Level.INFO, "All pods are started. Stopping to watch pod events. ");
            break;
          }
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
