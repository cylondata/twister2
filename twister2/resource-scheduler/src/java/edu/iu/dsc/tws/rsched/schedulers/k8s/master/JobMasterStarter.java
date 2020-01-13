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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKBarrierManager;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKEphemStateManager;
import edu.iu.dsc.tws.common.zk.ZKEventsManager;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.driver.K8sScaler;
import edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static edu.iu.dsc.tws.api.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class JobMasterStarter {
  private static final Logger LOG = Logger.getLogger(JobMasterStarter.class.getName());

  public static JobAPI.Job job;

  private JobMasterStarter() {
  }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // get environment variables
    String jobID = System.getenv(K8sEnvVariables.JOB_ID + "");
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "");
    String hostIP = System.getenv(K8sEnvVariables.HOST_IP + "");

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY;

    Config config = K8sWorkerUtils.loadConfig(configDir);

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

    // init logger
    K8sWorkerUtils.initLogger(config, "jobMaster");

    LOG.info("JobMaster is starting. Current time: " + System.currentTimeMillis());
    LOG.info("Number of configuration parameters: " + config.size());

    // get podIP from localhost
    InetAddress localHost = null;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get localHost.", e);
    }
    String podIP = localHost.getHostAddress();

    // construct nodeInfo for Job Master
    JobMasterAPI.NodeInfo nodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
        ? KubernetesContext.getNodeInfo(config, hostIP)
        : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, hostIP);

    LOG.info("NodeInfo for JobMaster: " + nodeInfo);

    // TODO: If ZooKeeper is not used,
    // currently we just return JM_STARTED. We do not determine real initial status.
    JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;
    if (ZKContext.isZooKeeperServerUsed(config)) {
      initialState = initializeZooKeeper(config, jobID, podIP);
    }

    JobTerminator jobTerminator = new JobTerminator(config);
    KubernetesController controller = new KubernetesController();
    controller.init(KubernetesContext.namespace(config));
    K8sScaler k8sScaler = new K8sScaler(config, job, controller);

    // start JobMaster
    JobMaster jobMaster =
        new JobMaster(config, podIP, jobTerminator, job, nodeInfo, k8sScaler, initialState);
    jobMaster.addShutdownHook(false);
    try {
      jobMaster.startJobMasterBlocking();
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }

    // wait to be deleted by K8s master
    K8sWorkerUtils.waitIndefinitely();
  }

  /**
   * Initialize zk directories if
   * <p>
   * Job Master is either starting for the first time, or it is coming from failure
   * if it is coming from failure, update jm pers state, initialize eventCounter, read job object,
   * return JobMasterState.JM_RESTARTED
   * <p>
   * if it is starting for the first time,
   * create job directories at zk, create pers state znode for jm, return JobMasterState.JM_STARTED
   */
  public static JobMasterAPI.JobMasterState initializeZooKeeper(Config config,
                                                                String jobID,
                                                                String jmAddress) {

    String zkServerAddresses = ZKContext.serverAddresses(config);
    int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
    CuratorFramework client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);
    String rootPath = ZKContext.rootNode(config);

    try {
      if (ZKPersStateManager.isJobMasterRestarting(client, rootPath, jobID, jmAddress)) {
        ZKEventsManager.initEventCounter(client, rootPath, jobID);
        job = ZKPersStateManager.readJobZNode(client, rootPath, jobID);
        return JobMasterAPI.JobMasterState.JM_RESTARTED;
      }

      // check if there is a job directory,
      // if so, that is a problem
      if (ZKUtils.isThereJobZNodes(client, rootPath, jobID)) {
        throw new Twister2RuntimeException("There is already a job znode at zookeeper for this job."
            + "Can not run this job.");
      }

      // if jm is not restarting, create job directories
      ZKEphemStateManager.createEphemDir(client, rootPath, job.getJobId());
      ZKEventsManager.createEventsZNode(client, rootPath, job.getJobId());
      ZKBarrierManager.createBarrierDir(client, rootPath, job.getJobId());
      ZKPersStateManager.createPersStateDir(client, rootPath, job);

      // create pers znode for jm
      ZKPersStateManager.createJobMasterPersState(client, rootPath, jobID, jmAddress);

      return JobMasterAPI.JobMasterState.JM_STARTED;

    } catch (Exception e) {
      throw new Twister2RuntimeException(e);
    }
  }

}
