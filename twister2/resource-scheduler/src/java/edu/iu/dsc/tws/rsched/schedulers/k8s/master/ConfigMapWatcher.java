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
package edu.iu.dsc.tws.rsched.schedulers.k8s.master;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.Watch;

/**
 * This thread watches ConfigMap events and
 * ends the job when KILL_JOB parameter is published on the ConfigMap
 * it is published when the user kills the job from twister2 client command line
 */

public class ConfigMapWatcher extends Thread {
  private static final Logger LOG = Logger.getLogger(ConfigMapWatcher.class.getName());

  private String namespace;
  private String jobID;
  private KubernetesController controller;
  private JobMaster jobMaster;

  public ConfigMapWatcher(String namespace,
                          String jobID,
                          KubernetesController controller,
                          JobMaster jobMaster) {
    this.namespace = namespace;
    this.jobID = jobID;
    this.controller = controller;
    this.jobMaster = jobMaster;
  }

  /**
   * start the watcher
   */
  @Override
  public void run() {
    String killParam = "KILL_JOB";

    String cmName = KubernetesUtils.createConfigMapName(jobID);
    String labelSelector = "t2-job=" + jobID;
    Integer timeoutSeconds = Integer.MAX_VALUE;

    CoreV1Api v1Api = controller.createCoreV1Api();

    Watch<V1ConfigMap> watcher;
    try {
      watcher = Watch.createWatch(
          controller.getApiClient(),
          v1Api.listNamespacedConfigMapCall(namespace, null, null, null, null, labelSelector,
              null, null, timeoutSeconds, Boolean.TRUE, null),
          new TypeToken<Watch.Response<V1ConfigMap>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the ConfigMap: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    try {

      for (Watch.Response<V1ConfigMap> item : watcher) {

        // if DeletionTimestamp is not null,
        // it means that the pod is in the process of being deleted
        if (item.object != null
            && item.object.getData() != null
            && item.object.getMetadata().getName().equals(cmName)
            && item.object.getData().get(killParam) != null) {

          LOG.info("Job Kill parameter received. Killing the job");
          jobMaster.endJob(JobAPI.JobState.KILLED);
          return;
        }
      }

    } finally {
      try {
        watcher.close();
      } catch (IOException e) {
        LOG.warning("IOException when closing ConfigMapWatcher");
      }
    }
  }
}
