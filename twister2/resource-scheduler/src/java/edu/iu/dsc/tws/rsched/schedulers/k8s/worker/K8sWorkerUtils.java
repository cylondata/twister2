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
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
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

  public static JobAPI.ComputeResource getComputeResource(JobAPI.Job job, String podName) {

    String ssName = KubernetesUtils.removeIndexFromName(podName);
    int currentStatefulSetIndex = KubernetesUtils.indexFromName(ssName);
    return JobUtils.getComputeResource(job, currentStatefulSetIndex);
  }


  /**
   * calculate the workerID from the given parameters
   */
  public static int calculateWorkerID(JobAPI.Job job, String podName, String containerName) {

    String ssName = KubernetesUtils.removeIndexFromName(podName);
    int currentStatefulSetIndex = KubernetesUtils.indexFromName(ssName);
    int workersUpToSS = countWorkersUpToSS(job, currentStatefulSetIndex);

    int podIndex = KubernetesUtils.indexFromName(podName);
    int containerIndex = KubernetesUtils.indexFromName(containerName);
    int workersPerPod =
        JobUtils.getComputeResource(job, currentStatefulSetIndex).getWorkersPerPod();

    int workerID = workersUpToSS + calculateWorkerIDInSS(podIndex, containerIndex, workersPerPod);
    return workerID;
  }

  /**
   * calculate the number of workers in the earlier statefulsets
   */
  public static int countWorkersUpToSS(JobAPI.Job job, int currentStatefulSetIndex) {

    int workerCount = 0;
    for (int i = 0; i < currentStatefulSetIndex; i++) {
      workerCount += JobUtils.getComputeResource(job, i).getNumberOfWorkers();
    }

    return workerCount;
  }

  /**
   * calculate the workerID in the given StatefulSet
   */
  public static int calculateWorkerIDInSS(int podIndex, int containerIndex, int workersPerPod) {
    return podIndex * workersPerPod + containerIndex;
  }

  public static JobMasterAPI.NodeInfo getNodeInfoFromEncodedStr(String encodedNodeInfoList,
                                                                String nodeIP) {

    // we will return this, in case we do not find it in the given list
    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo(nodeIP, null, null);

    ArrayList<JobMasterAPI.NodeInfo> nodeInfoList =
        NodeInfoUtils.decodeNodeInfoList(encodedNodeInfoList);

    if (nodeInfoList == null || nodeInfoList.size() == 0) {
      LOG.warning("NodeInfo list is not constructed from the string: " + encodedNodeInfoList);
      return nodeInfo;
    } else {
      LOG.fine("Decoded NodeInfo list, size: " + nodeInfoList.size()
          + "\n" + NodeInfoUtils.listToString(nodeInfoList));

      JobMasterAPI.NodeInfo nodeInfo1 = NodeInfoUtils.getNodeInfo(nodeInfoList, nodeIP);
      if (nodeInfo1 == null) {
        LOG.warning("nodeIP does not exist in received encodedNodeInfoList. Using local value.");
        return nodeInfo;
      }

      return nodeInfo1;
    }
  }

  /**
   * get job master service IP from job master service name
   * @param jobName
   * @return
   */
  public static String getJobMasterServiceIP(String namespace, String jobName) {
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
