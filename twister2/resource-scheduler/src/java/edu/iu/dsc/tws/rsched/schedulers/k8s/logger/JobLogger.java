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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
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
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

/**
 * get logs of all workers and the job master in a job from Kubernetes master
 * save them to local disk under the directory:
 *   $HOME/.twister2/jobID
 */

public class JobLogger extends Thread {
  private static final Logger LOG = Logger.getLogger(JobLogger.class.getName());

  private JobAPI.Job job;
  private String namespace;
  private String logsDir;

  private CoreV1Api v1Api;
  private Watch<V1Pod> watcher;
  private boolean stopLogger = false;

  private List<WorkerLogger> loggers;
  private Set<String> completedLoggers;

  private int numberOfWorkers;

  public JobLogger(String namespace, JobAPI.Job job) {
    this.namespace = namespace;
    this.job = job;
    this.numberOfWorkers = job.getNumberOfWorkers();

    loggers = new LinkedList<>();
    completedLoggers = new ConcurrentSkipListSet<>();
  }

  @Override
  public void run() {
    v1Api = KubernetesController.createCoreV1Api();
    logsDir = System.getProperty("user.home") + "/.twister2/" + job.getJobId();
    if (FileUtils.isDirectoryExists(logsDir)) {
      logsDir = getAvailableLogDir(logsDir);
    }
    FileUtils.createDirectory(logsDir);
    LOG.info("Job logs directory: " + logsDir);

    watchPodsToRunningStartLoggers();
  }

  /**
   * if there is already a directory with the same name
   * get an available directory name with a numerical suffix added
   * if there is also a directory with the same numerical suffix,
   * increase the suffix value until finding an available one
   * @param existingDir
   */
  private String getAvailableLogDir(String existingDir) {
    int suffix = 1;
    String dirName = existingDir + "-" + suffix;
    while (FileUtils.isDirectoryExists(dirName)) {
      suffix++;
      dirName = existingDir + "-" + suffix;
    }
    return dirName;
  }

  /**
   * worker loggers let JobLogger know that they completed
   * JobLogger needs to close watcher and finish log saving
   */
  public synchronized void workerLoggerCompleted(String loggerID) {
    completedLoggers.add(loggerID);

    if (completedLoggers.size() == numberOfWorkers + 1) {
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

    String jobPodsLabel = KubernetesUtils.jobLabelSelector(job.getJobId());

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
          String podIP = item.object.getStatus().getPodIP();

          List<V1Container> containers = item.object.getSpec().getContainers();
          if (podName.endsWith("-jm-0")) {
            String contName = containers.get(0).getName();
            String id = "job-master-ip" + podIP;
            WorkerLogger workerLogger = new WorkerLogger(
                namespace, podName, contName, id, logsDir, v1Api, this);
            startWorkerLogger(workerLogger);
            continue;
          }

          for (V1Container container : containers) {
            int wID = K8sWorkerUtils.calculateWorkerID(job, podName, container.getName());
            // this means job is scaled up
            if (wID >= numberOfWorkers) {
              numberOfWorkers = wID + 1;
            }
            String id = "worker" + wID + "-ip" + podIP;
            WorkerLogger workerLogger =
                new WorkerLogger(namespace, podName, container.getName(), id, logsDir, v1Api, this);
            startWorkerLogger(workerLogger);
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

  private void startWorkerLogger(WorkerLogger workerLogger) {
    if (loggers.contains(workerLogger)) {
      WorkerLogger existingLogger = loggers.get(loggers.indexOf(workerLogger));
      if (existingLogger.isAlive()) {
        // ignore the event, it is double event for the same worker
        LOG.info("Ignoring " + workerLogger.getID() + " start event for logging, "
            + "since a logger for that worker is already running.");
        return;
      }
    }
    workerLogger.start();
    completedLoggers.removeIf(loggerID -> loggerID.equals(workerLogger.getID()));
    loggers.add(workerLogger);
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
