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

import java.util.HashMap;
import java.util.logging.Logger;

/**
 * a class to watch events related to pods starting containers in them
 * this thread starts watching events for all pods in a job
 * when a container in a pod becomes Started, it marks that pod as ready
 * when all pods become ready, this thread stops watching events and finishes execution
 */

public class PodWatcher extends Thread {
  private static final Logger LOG = Logger.getLogger(PodWatcher.class.getName());

  private HashMap<String, Boolean> pods;
  private String namespace;
  private String jobName;

  public PodWatcher(String namespace, String jobName, int numberOfPods) {

    this.namespace = namespace;
    this.jobName = jobName;

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
   * we can also watch for a pod Running event, however that usually takes much longer
   * When we use Container Started event, our first try to transfer the file may fail,
   * we retry it, and usually it succeeds after a few tries.
   *
   * we conducted some tests and compared these two approaches
   * transferring files after Container Started event is usually much faster
   */
  public void run() {

    PodWatchUtils.watchPodsToStarting(namespace, pods, 50);
//    PodWatchUtils.watchPodsToRunning(namespace, jobName, pods, 50);

  }
}
