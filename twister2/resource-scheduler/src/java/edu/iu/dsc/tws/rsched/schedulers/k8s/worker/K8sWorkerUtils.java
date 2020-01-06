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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
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
    Config conf1 = ConfigLoader.loadConfig(twister2Home, configDir, KUBERNETES_CLUSTER_TYPE);
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
   * initialize the logger
   */
  public static void initWorkerLogger(int workerID, K8sPersistentVolume pv, Config cnfg) {

    if (pv != null && LoggingContext.fileLoggingRequested()) {

      if (LoggingContext.redirectSysOutErr()) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      String logFile = K8sPersistentVolume.WORKER_LOG_FILE_NAME_PREFIX + workerID;
      LoggingHelper.setupLogging(cnfg, pv.getLogDirPath(), logFile);

      LOG.info("Persistent logging to file initialized.");
    }
  }

  /**
   * initialize the logger
   * entityName can be "jobMaster", "mpiMaster", etc.
   */
  public static void initLogger(Config cnfg, String entityName) {
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
    if (LoggingContext.fileLoggingRequested()) {

      if (LoggingContext.redirectSysOutErr()) {
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
      JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, i);
      workerCount += computeResource.getInstances() * computeResource.getWorkersPerPod();
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
   */
  public static String getJobMasterServiceIP(String namespace, String jobID) {
    String jobMasterServiceName = KubernetesUtils.createJobMasterServiceName(jobID);
    jobMasterServiceName = jobMasterServiceName + "." + namespace + ".svc.cluster.local";
    try {
      return InetAddress.getByName(jobMasterServiceName).getHostAddress();
    } catch (UnknownHostException e) {
      LOG.info("Cannot get Job master IP from service name: " + jobMasterServiceName);
      return null;
    }
  }

  /**
   * get job master service IP from job master service name
   * poll repeatedly until getting it or times out
   */
  public static String getJobMasterServiceIPByPolling(String namespace,
                                                      String jobID,
                                                      long timeLimitMS) {
    String jmServiceName = KubernetesUtils.createJobMasterServiceName(jobID);
    jmServiceName = jmServiceName + "." + namespace + ".svc.cluster.local";

    long sleepInterval = 100;

    long startTime = System.currentTimeMillis();
    long duration = 0;

    // log interval in milliseconds
    long logInterval = 1000;
    long nextLogTime = logInterval;

    while (duration < timeLimitMS) {

      // try getting job master IP address
      try {
        InetAddress jmAddress = InetAddress.getByName(jmServiceName);
        return jmAddress.getHostAddress();
      } catch (UnknownHostException e) {
        LOG.fine("Cannot get Job master IP from service name.");
      }

      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted.");
      }

      // increase sleep interval by 10 in every iteration
      sleepInterval += 10;

      duration = System.currentTimeMillis() - startTime;

      if (duration > nextLogTime) {
        LOG.info("Still trying to get Job Master IP address for the service:  " + jmServiceName);
        nextLogTime += logInterval;
      }
    }

    return null;
  }

  /**
   * generate the additional requested ports for this worker
   */
  public static Map<String, Integer> generateAdditionalPorts(Config config, int workerPort) {

    // if no port is requested, return null
    List<String> portNames = SchedulerContext.additionalPorts(config);
    if (portNames == null) {
      return null;
    }

    HashMap<String, Integer> ports = new HashMap<>();
    int i = 1;
    for (String portName : portNames) {
      ports.put(portName, workerPort + i++);
    }

    return ports;
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

  /**
   * worker is either starting for the first time, or it is coming from failure
   * We return either WorkerState.STARTED or WorkerState.RESTARTED
   *
   * TODO: If ZooKeeper is not used,
   *   currently we just return STARTED. We do not determine real initial status.
   * @return
   */
  public static JobMasterAPI.WorkerState initialStateAndUpdate(Config cnfg,
                                                               String jbID,
                                                               JobMasterAPI.WorkerInfo wInfo) {

    if (ZKContext.isZooKeeperServerUsed(cnfg)) {
      String zkServerAddresses = ZKContext.serverAddresses(cnfg);
      int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(cnfg);
      CuratorFramework client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);
      String rootPath = ZKContext.rootNode(cnfg);

      try {
        if (ZKPersStateManager.initWorkerPersState(client, rootPath, jbID, wInfo)) {
          return JobMasterAPI.WorkerState.RESTARTED;
        }

        return JobMasterAPI.WorkerState.STARTED;

      } catch (Exception e) {
        LOG.log(Level.SEVERE,
            "Could not get initial state for the worker. Assuming WorkerState.STARTED", e);
        return JobMasterAPI.WorkerState.STARTED;
      }
    }

    return JobMasterAPI.WorkerState.STARTED;
  }
}
