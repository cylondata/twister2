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
package edu.iu.dsc.tws.rsched.schedulers.k8s.mpi;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.ISenderToDriver;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sPersistentVolume;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sVolatileVolume;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import mpi.MPI;
import mpi.MPIException;
import static edu.iu.dsc.tws.api.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class MPIWorkerStarter {
  private static final Logger LOG = Logger.getLogger(MPIWorkerStarter.class.getName());

  private static Config config = null;
  private static int workerID = -1; // -1 means, not initialized
  private static int numberOfWorkers = -1; // -1 means, not initialized
  private static JobMasterAPI.WorkerInfo workerInfo;
  private static String jobID = null;
  private static JobAPI.Job job = null;
  private static JobAPI.ComputeResource computeResource = null;

  private MPIWorkerStarter() {
  }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    String jobMasterIP = MPIMasterStarter.getJobMasterIPCommandLineArgumentValue(args[0]);
    jobID = args[1];
    String encodedNodeInfoList = args[2];

    if (jobMasterIP == null) {
      throw new RuntimeException("JobMasterIP address is null");
    }

    if (jobID == null) {
      throw new RuntimeException("jobID is null");
    }

    // remove the first and the last single quotas from encodedNodeInfoList
    encodedNodeInfoList = encodedNodeInfoList.replaceAll("'", "");

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY;

    config = K8sWorkerUtils.loadConfig(configDir);

    // initialize MPI
    try {
      MPI.Init(args);
      workerID = MPI.COMM_WORLD.getRank();
      numberOfWorkers = MPI.COMM_WORLD.getSize();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Could not get rank or size from MPI.COMM_WORLD", e);
      throw new RuntimeException(e);
    }

    // initialize persistent volume
    K8sPersistentVolume pv = null;
    if (KubernetesContext.persistentVolumeRequested(config)) {
      // create persistent volume object
      String persistentJobDir = KubernetesConstants.PERSISTENT_VOLUME_MOUNT;
      pv = new K8sPersistentVolume(persistentJobDir, workerID);
    }

    // initialize persistent logging
    K8sWorkerUtils.initWorkerLogger(workerID, pv, config);

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobID);
    jobDescFileName = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/" + jobDescFileName;
    job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

    // update jobMasterIP
    // update config with jobMasterIP
    config = Config.newBuilder()
        .putAll(config)
        .put(JobMasterContext.JOB_MASTER_IP, jobMasterIP)
        .build();

    InetAddress localHost = null;
    String podName = null;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Cannot get localHost.", e);
    }

    String podIP = localHost.getHostAddress();
    podName = localHost.getHostName();
    // podName may be in the form of: t2-job-0-0.t2-srv-t2-job.default.svc.cluster.local
    // get up to first dot: t2-job-0-0
    if (podName.indexOf(".") > 0) {
      podName = podName.substring(0, podName.indexOf("."));
    }

    int workerPort = KubernetesContext.workerBasePort(config)
        + workerID * (SchedulerContext.numberOfAdditionalPorts(config) + 1);

    String nodeIP = PodWatchUtils.getNodeIP(KubernetesContext.namespace(config), jobID, podIP);
    JobMasterAPI.NodeInfo nodeInfo = null;
    if (nodeIP == null) {
      LOG.warning("Could not get nodeIP for this pod. Using podIP as nodeIP.");
      nodeInfo = NodeInfoUtils.createNodeInfo(podIP, null, null);
    } else {

      nodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
          ? KubernetesContext.getNodeInfo(config, nodeIP)
          : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, nodeIP);
    }

    LOG.info(String.format("PodName: %s, NodeInfo for this worker: %s", podName, nodeInfo));

    computeResource = K8sWorkerUtils.getComputeResource(job, podName);

    // generate additional ports if requested
    Map<String, Integer> additionalPorts =
        K8sWorkerUtils.generateAdditionalPorts(config, workerPort);

    workerInfo = WorkerInfoUtils.createWorkerInfo(
        workerID, podIP, workerPort, nodeInfo, computeResource, additionalPorts);

    LOG.info("Worker information summary: \n"
        + "MPI Rank(workerID): " + workerID + "\n"
        + "MPI Size(number of workers): " + numberOfWorkers + "\n"
        + "POD_IP: " + podIP + "\n"
        + "HOSTNAME(podname): " + podName
    );

    JobMasterAPI.WorkerState initialState =
        K8sWorkerUtils.initialStateAndUpdate(config, jobID, workerInfo);
    WorkerRuntime.init(config, job, workerInfo, initialState);

    /**
     * Interfaces to interact with other workers and Job Master if there is any
     */
    IWorkerController workerController = WorkerRuntime.getWorkerController();
    IWorkerStatusUpdater workerStatusUpdater = WorkerRuntime.getWorkerStatusUpdater();
    ISenderToDriver senderToDriver = WorkerRuntime.getSenderToDriver();

    // start the worker
    startWorker(workerController, pv, podName);

    // update worker status to COMPLETED
    workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.COMPLETED);

    // finalize MPI
    try {
      MPI.Finalize();
    } catch (MPIException ignore) {
    }

    WorkerRuntime.close();
  }

  /**
   * start the Worker class specified in conf files
   */
  public static void startWorker(IWorkerController workerController,
                                 IPersistentVolume pv, String podName) {
    String workerClass = SchedulerContext.workerClass(config);
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the worker class %s", workerClass));
      throw new RuntimeException(e);
    }

    K8sVolatileVolume volatileVolume = null;
    if (computeResource.getDiskGigaBytes() > 0) {
      volatileVolume = new K8sVolatileVolume(jobID, workerID);
    }

    worker.execute(config, workerID, workerController, pv, volatileVolume);
  }
}
