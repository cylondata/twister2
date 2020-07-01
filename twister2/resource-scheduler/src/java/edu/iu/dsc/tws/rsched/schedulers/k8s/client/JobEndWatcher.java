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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;

/**
 * watch until the job ends
 * when the job ends, let DirectUploader stop
 */

public final class JobEndWatcher extends Thread {
  private static final Logger LOG = Logger.getLogger(JobEndWatcher.class.getName());

  private String jobID;
  private KubernetesController controller;

  private boolean stop = false;
  private List<JobEndListener> jobEndListeners = Collections.synchronizedList(new LinkedList<>());

  private static JobEndWatcher jobEndWatcher;

  // how often we should check with k8s master
  private static final long CHECK_INTERVAL = 1000;

  public static synchronized JobEndWatcher init(String namespace, String jobID) {
    if (jobEndWatcher != null) {
      return jobEndWatcher;
    }

    jobEndWatcher = new JobEndWatcher(namespace, jobID);
    jobEndWatcher.start();

    return jobEndWatcher;
  }

  private JobEndWatcher(String namespace, String jobID) {
    this.controller = KubernetesController.init(namespace);
    this.jobID = jobID;
  }

  /**
   * add a listener
   * @param jobEndListener
   */
  public void addJobEndListener(JobEndListener jobEndListener) {
    jobEndListeners.add(jobEndListener);
  }

  /**
   * start the watcher
   */
  @Override
  public void run() {

    while (!stop) {

      try {
        // sleep 1 second between queries
        Thread.sleep(CHECK_INTERVAL);
      } catch (InterruptedException e) {
      }

      if (!stop && controller.getJobConfigMap(jobID) == null) {
        // job has ended
        // log "job has ended" message if the client logger is not used
        jobEndListeners.forEach(JobEndListener::jobEnded);
        LOG.info("Job ended...");
        return;
      }
    }
  }

  public void stopWatcher() {
    stop = true;
    jobEndWatcher.interrupt();
  }

}
