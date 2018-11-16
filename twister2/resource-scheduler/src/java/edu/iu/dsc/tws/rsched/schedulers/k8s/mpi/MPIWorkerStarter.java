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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfoUtil;
import edu.iu.dsc.tws.common.discovery.WorkerInfoUtil;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sPersistentVolume;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sVolatileVolume;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import mpi.MPI;
import mpi.MPIException;

import static edu.iu.dsc.tws.common.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class MPIWorkerStarter {
  private static final Logger LOG = Logger.getLogger(MPIWorkerStarter.class.getName());

  private static Config config = null;
  private static int workerID = -1; // -1 means, not initialized
  private static int numberOfWorkers = -1; // -1 means, not initialized
  private static JobMasterAPI.WorkerInfo workerInfo;
  private static JobMasterClient jobMasterClient;
  private static String jobName = null;
  private static JobAPI.Job job = null;
  private static JobAPI.ComputeResource computeResource = null;

  private MPIWorkerStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    String jobMasterIP = MPIMasterStarter.getJobMasterIPCommandLineArgumentValue(args[0]);
    jobName = args[1];
    String encodedNodeInfoList = args[2];

    if (jobMasterIP == null) {
      throw new RuntimeException("JobMasterIP address is null");
    }

    if (jobName == null) {
      throw new RuntimeException("jobName is null");
    }

    // remove the first and the last single quotas from encodedNodeInfoList
    encodedNodeInfoList = encodedNodeInfoList.replaceAll("'", "");

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/"
        + KUBERNETES_CLUSTER_TYPE;

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
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobName);
    jobDescFileName = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/" + jobDescFileName;
    job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

    InetAddress localHost = null;
    String podName = null;
    try {
      localHost = InetAddress.getLocalHost();
      String podIP = localHost.getHostAddress();
      podName = localHost.getHostName();

      int workerPort = KubernetesContext.workerBasePort(config) + workerID;

      String nodeIP = PodWatchUtils.getNodeIP(KubernetesContext.namespace(config), jobName, podIP);
      JobMasterAPI.NodeInfo nodeInfo = null;
      if (nodeIP == null) {
        LOG.warning("Could not get nodeIP for this pod. Using podIP as nodeIP.");
        nodeInfo = NodeInfoUtil.createNodeInfo(podIP, null, null);
      } else {

        nodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
            ? KubernetesContext.getNodeInfo(config, nodeIP)
            : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, nodeIP);
      }

      LOG.info("NodeInfoUtil for this worker: " + nodeInfo);

      computeResource = K8sWorkerUtils.getComputeResource(job, podName);

      workerInfo = WorkerInfoUtil.createWorkerInfo(
          workerID, localHost.getHostAddress(), workerPort, nodeInfo, computeResource);

      LOG.info("Worker information summary: \n"
          + "MPI Rank(workerID): " + workerID + "\n"
          + "MPI Size(number of workers): " + numberOfWorkers + "\n"
          + "POD_IP: " + podIP + "\n"
          + "HOSTNAME(podname): " + podName
      );
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Cannot get localHost.", e);
    }

    // start JobMasterClient
    jobMasterClient = new JobMasterClient(config, workerInfo, jobMasterIP,
        JobMasterContext.jobMasterPort(config), job.getNumberOfWorkers());

    Thread clientThread = jobMasterClient.startThreaded();
    if (clientThread == null) {
      throw new RuntimeException("Can not start JobMasterClient thread.");
    }

    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();

    // we will be running the Worker, send running message
    jobMasterClient.sendWorkerRunningMessage();

    // start the worker
    startWorker(jobMasterClient.getJMWorkerController(), pv, podName);

    // finalize MPI
    try {
      MPI.Finalize();
    } catch (MPIException ignore) { }

    // close the worker
    closeWorker();
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

    AllocatedResources allocatedResources = null;
//    AllocatedResources allocatedResources = K8sWorkerUtils.createAllocatedResources(
//        KubernetesContext.clusterType(config), workerID, job);

    K8sVolatileVolume volatileVolume = null;
    if (computeResource.getDiskGigaBytes() > 0) {
      volatileVolume = new K8sVolatileVolume(jobName, workerID);
    }

    worker.execute(config, workerID, allocatedResources, workerController, pv, volatileVolume);
  }

  /**
   * last method to call to close the worker
   */
  public static void closeWorker() {

    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    jobMasterClient.sendWorkerCompletedMessage();
    jobMasterClient.close();
  }

}
