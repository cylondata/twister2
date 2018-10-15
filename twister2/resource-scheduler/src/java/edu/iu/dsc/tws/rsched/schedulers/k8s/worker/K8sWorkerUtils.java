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

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;

public final class K8sWorkerUtils {
  private static final Logger LOG = Logger.getLogger(K8sWorkerUtils.class.getName());

  private K8sWorkerUtils() {
  }

  /**
   * load configuration files from the given directory
   */
  public static Config loadConfig(String configDir) {

    // we assume that the twister2Home is the current directory
    String twister2Home = Paths.get("").toAbsolutePath().toString();

    LOG.info(String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s", twister2Home, configDir));
    Config conf1 = ConfigLoader.loadConfig(twister2Home, configDir);
    LOG.info("Loaded: " + conf1.size() + " parameters from configuration directory: " + configDir);

    Config conf2 = Config.newBuilder().
        putAll(conf1).
        put(Context.TWISTER2_HOME.getKey(), twister2Home).
        put(Context.TWISTER2_CONF.getKey(), configDir).
        put(Context.TWISTER2_CLUSTER_TYPE, KUBERNETES_CLUSTER_TYPE).
        build();

    return conf2;
  }

  /**
   * itinialize the logger
   */
  public static void initWorkerLogger(int workerID, K8sPersistentVolume pv, Config cnfg) {

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    // if persistent logging is requested, initialize it
    if (pv != null && LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      String logFile = K8sPersistentVolume.WORKER_LOG_FILE_NAME_PREFIX + workerID;
      LoggingHelper.setupLogging(cnfg, pv.getLogDirPath(), logFile);

      LOG.info("Persistent logging to file initialized.");
    }
  }

  /**
   * itinialize the logger
   * entityName can be "jobMaster", "mpiMaster", etc.
   */
  public static void initLogger(Config cnfg, String entityName) {
    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    // if no persistent volume requested, return
    if ("jobMaster".equalsIgnoreCase(entityName)
        && !JobMasterContext.persistentVolumeRequested(cnfg)) {
      return;
    }

    if ("mpiMaster".equalsIgnoreCase(entityName)
        && !KubernetesContext.persistentVolumeRequested(cnfg)) {
      return;
    }

    // if persistent logging is requested, initialize it
    if (LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      String logDirName = KubernetesConstants.PERSISTENT_VOLUME_MOUNT + "/logs";
      File logDir = new File(logDirName);

      // refresh parent directory the cache
      logDir.getParentFile().list();

      if (!logDir.exists()) {
        logDir.mkdirs();
      }

      String logFileName = entityName;

      LoggingHelper.setupLogging(cnfg, logDirName, logFileName);

      String logFileWithPath = logDirName + "/" + logFileName + ".log.0";
      LOG.info("Persistent logging to file initialized: " + logFileWithPath);
    }
  }

  /**
   * calculate the workerID from the given parameters
   */
  public static int calculateWorkerID(String podName, String containerName, int workersPerPod) {
    int podIndex = KubernetesUtils.idFromName(podName);
    int containerIndex = KubernetesUtils.idFromName(containerName);

    return calculateWorkerID(podIndex, containerIndex, workersPerPod);
  }

  /**
   * calculate the workerID from the given parameters
   */
  public static int calculateWorkerID(int podIndex, int containerIndex, int workersPerPod) {
    return podIndex * workersPerPod + containerIndex;
  }

  public static NodeInfo getNodeInfoFromEncodedStr(String encodedNodeInfoList, String nodeIP) {
    NodeInfo nodeInfo = new NodeInfo(nodeIP, null, null);
    ArrayList<NodeInfo> nodeInfoList = NodeInfo.decodeNodeInfoList(encodedNodeInfoList);

    if (nodeInfoList == null || nodeInfoList.size() == 0) {
      LOG.warning("NodeInfo list is not constructed from the string: " + encodedNodeInfoList);
    } else {
      LOG.fine("Decoded NodeInfo list, size: " + nodeInfoList.size()
          + "\n" + NodeInfo.listToString(nodeInfoList));

      int index = nodeInfoList.indexOf(nodeInfo);
      if (index < 0) {
        LOG.warning("nodeIP does not exist in received encodedNodeInfoList. Using local value.");
        return nodeInfo;
      }
      nodeInfo = nodeInfoList.get(index);
    }

    return nodeInfo;
  }

  /**
   * we assume all resources are uniform
   * @return
   */
  public static AllocatedResources createAllocatedResources(String cluster,
                                                            int workerID,
                                                            JobAPI.Job job) {

    JobAPI.WorkerComputeResource computeResource =
        job.getJobResources().getResources(0).getWorkerComputeResource();

    AllocatedResources allocatedResources = new AllocatedResources(cluster, workerID);

    for (int i = 0; i < job.getNumberOfWorkers(); i++) {
      allocatedResources.addWorkerComputeResource(new WorkerComputeResource(
          i, computeResource.getCpu(), computeResource.getRam(), computeResource.getDisk()));
    }

    return allocatedResources;
  }

  /**
   * get job master service IP from job master service name
   * @param jobName
   * @return
   */
  public static String getJobMasterServiceIP(String jobName, String namespace) {
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobName);
    jobMasterServiceName = jobMasterServiceName + "." + namespace + ".svc.cluster.local";
    try {
      return InetAddress.getByName(jobMasterServiceName).getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get Job master IP from service name.", e);
    }
  }


  /**
   * a test method to make the worker wait indefinitely
   */
  public static void waitIndefinitely() {

    while (true) {
      try {
        LOG.info("Worker completed. Waiting idly to be deleted by Job Master. Sleeping 100sec. "
            + "Time: " + new java.util.Date());
        Thread.sleep(100000);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }
  }

}
