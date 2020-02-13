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
package edu.iu.dsc.tws.rsched.schedulers.k8s.logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.FileUtils;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

/**
 * get logs of all workers in a job from Kubernetes master
 * save it to local disk
 */

public class JobLogger extends Thread {
  private static final Logger LOG = Logger.getLogger(JobLogger.class.getName());

  private JobAPI.Job job;
  private String namespace;
  private List<String> containerNames;
  private String logsDir;

  private CoreV1Api v1Api;
  private Watch<V1Pod> watcher;
  private boolean stopLogger = false;

  private List<WorkerLogger> loggers;
  private int completedLoggerCounter = 0;

  public JobLogger(String namespace, JobAPI.Job job) {
    this.job = job;
    this.namespace = namespace;

    loggers = new LinkedList<>();

    containerNames = new LinkedList<>();
    for (int i = 0; i < job.getComputeResource(0).getWorkersPerPod(); i++) {
      containerNames.add(KubernetesUtils.createContainerName(i));
    }
  }

  @Override
  public void run() {
    v1Api = KubernetesController.createCoreV1Api();
    logsDir = System.getProperty("user.home") + "/.twister2/" + job.getJobId();
    if (!FileUtils.isDirectoryExists(logsDir)) {
      FileUtils.createDirectory(logsDir);
    }
    LOG.info("Job logs directory: " + logsDir);

    watchPodsToRunningStartLoggers();
  }

  /**
   * TODO: need to take into account restarts and scaling up/down
   * @param workerLogger
   */
  public void workerLoggerCompleted(WorkerLogger workerLogger) {
    completedLoggerCounter++;
    loggers.remove(workerLogger);

    if (completedLoggerCounter == job.getNumberOfWorkers() + 1) {
      stopLogger();
      LOG.info("All workers completed. Job has finished.");
    }
  }

  /**
   * watch job pods until they become Running and start loggers for each container afterward
   */
  private void watchPodsToRunningStartLoggers() {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(job.getJobId());

    Integer timeoutSeconds = Integer.MAX_VALUE;
    String podPhase = "Running";

    try {
      watcher = Watch.createWatch(
          KubernetesController.getApiClient(),
          v1Api.listNamespacedPodCall(namespace, null, null, null, null, jobPodsLabel,
              null, null, timeoutSeconds, Boolean.TRUE, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the pods to get the IPs: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    // when we close the watcher to stop uploader,
    // it throws RuntimeException
    // we catch this exception and ignore it.
    try {

      for (Watch.Response<V1Pod> item : watcher) {

        if (stopLogger) {
          break;
        }

        // if DeletionTimestamp is not null,
        // it means that the pod is in the process of being deleted
        if (item.object != null
            && item.object.getMetadata().getName().startsWith(job.getJobId())
            && podPhase.equals(item.object.getStatus().getPhase())
            && item.object.getMetadata().getDeletionTimestamp() == null
        ) {

          String podName = item.object.getMetadata().getName();
          if (podName.endsWith("-jm-0")) {
            WorkerLogger workerLogger = new WorkerLogger(
                namespace, podName, "twister2-job-master-0", "job-master", logsDir, v1Api, this);
            workerLogger.start();
            loggers.add(workerLogger);
            continue;
          }

          for (String cont: containerNames) {
            String id = "worker-" + K8sWorkerUtils.calculateWorkerID(job, podName, cont);
            WorkerLogger workerLogger =
                new WorkerLogger(namespace, podName, cont, id, logsDir, v1Api, this);
            workerLogger.start();
            loggers.add(workerLogger);
          }

        }
      }

    } catch (RuntimeException e) {
      if (stopLogger) {
        LOG.fine("JobLogger is stopped.");
        return;
      } else {
        throw e;
      }
    }

    closeWatcher();
  }

  private void closeWatcher() {

    if (watcher == null) {
      return;
    }

    try {
      watcher.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

    watcher = null;
  }

  public void stopLogger() {
    stopLogger = true;
    closeWatcher();

    for (WorkerLogger logger: loggers) {
      if (logger.isAlive()) {
        logger.stopLogging();
      }
    }
  }
}
