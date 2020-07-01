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

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.JobZNodeManager;
import edu.iu.dsc.tws.common.zk.ZKBarrierManager;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKEphemStateManager;
import edu.iu.dsc.tws.common.zk.ZKEventsManager;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
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
    String jobID = System.getenv(K8sEnvVariables.JOB_ID.name());
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST.name());
    String hostIP = System.getenv(K8sEnvVariables.HOST_IP.name());
    boolean restoreJob = Boolean.parseBoolean(System.getenv(K8sEnvVariables.RESTORE_JOB.name()));

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + File.separator + JOB_ARCHIVE_DIRECTORY;

    Config config = K8sWorkerUtils.loadConfig(configDir);

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobID);
    jobDescFileName = POD_MEMORY_VOLUME + File.separator + JOB_ARCHIVE_DIRECTORY
        + File.separator + jobDescFileName;
    job = JobUtils.readJobFile(jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);
    config = Config.newBuilder().putAll(config)
        .put(CheckpointingContext.CHECKPOINTING_RESTORE_JOB, restoreJob)
        .build();

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

    KubernetesController controller =
        KubernetesController.init(KubernetesContext.namespace(config));
    JobTerminator jobTerminator = new JobTerminator(config, controller);
    K8sScaler k8sScaler = new K8sScaler(config, job, controller);

    // get restart count from job ConfigMap
    // if jm is running for the first time, initialize restart count at CM
    String keyName = KubernetesUtils.createRestartJobMasterKey();
    int restartCount = K8sWorkerUtils.initRestartFromCM(controller, jobID, keyName);

    JobMasterState initialState = JobMasterState.JM_STARTED;
    if (restartCount > 0) {
      initialState = JobMasterState.JM_RESTARTED;

      // if zookeeper is not used, jm can not recover from failure
      // so, we terminate the job with failure
      if (!ZKContext.isZooKeeperServerUsed(config)) {
        jobTerminator.terminateJob(jobID, JobAPI.JobState.FAILED);
        return;
      }
    }

    if (ZKContext.isZooKeeperServerUsed(config)) {
      boolean zkInitialized = initializeZooKeeper(config, jobID, podIP, initialState);
      if (!zkInitialized) {
        jobTerminator.terminateJob(jobID, JobAPI.JobState.FAILED);
        return;
      }
    }

    // start JobMaster
    JobMaster jobMaster =
        new JobMaster(config, podIP, jobTerminator, job, nodeInfo, k8sScaler, initialState);

    // start configMap watcher to watch the kill parameter from twister2 client
    JobKillWatcher jkWatcher =
        new JobKillWatcher(KubernetesContext.namespace(config), jobID, controller, jobMaster);
    jkWatcher.start();

    // on any uncaught exception, we will label the job master as FAILED and
    // throw a RuntimeException
    // JVM will be restarted by K8s
    Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
      LOG.log(Level.SEVERE, "Uncaught exception in the thread "
          + thread + ". Job Master FAILED...", throwable);

      jobMaster.jmFailed();
      jkWatcher.close();
      controller.close();
      throw new RuntimeException("Worker failed with the exception", throwable);
    });

    try {
      jobMaster.startJobMasterBlocking();
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }

    // close the controller
    controller.close();
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
  public static boolean initializeZooKeeper(Config config,
                                            String jobID,
                                            String jmAddress,
                                            JobMasterState initialState) {

    String zkServerAddresses = ZKContext.serverAddresses(config);
    int sessionTimeoutMs = FaultToleranceContext.sessionTimeout(config);
    CuratorFramework client = ZKUtils.connectToServer(zkServerAddresses, sessionTimeoutMs);
    String rootPath = ZKContext.rootNode(config);

    // check whether the job znode exists
    boolean jobZNodeExists = JobZNodeManager.isThereJobZNode(client, rootPath, jobID);

    // handle the jm restart case
    if (initialState == JobMasterState.JM_RESTARTED) {
      // if the job is restarting and job znode does not exist, that is a failure

      if (!jobZNodeExists) {
        LOG.severe("Job is restarting but job znode does not exists at ZK server at: "
            + ZKUtils.jobDir(rootPath, jobID));
        return false;
      }

      ZKPersStateManager.updateJobMasterStatus(
          client, rootPath, jobID, jmAddress, JobMasterState.JM_RESTARTED);
      ZKEventsManager.initEventCounter(client, rootPath, jobID);
      job = JobZNodeManager.readJobZNode(client, rootPath, jobID).getJob();
      return true;
    }

    // handle JM first start case

    // if the job is not going to continue from a checkpoint,
    // there can not be an existing job znode for this job,
    // if there is, that is a reason for failure
    if (!CheckpointingContext.startingFromACheckpoint(config)
        && jobZNodeExists) {
      LOG.severe("Job is starting for the first time, but there is an existing znode at ZK server: "
          + ZKUtils.jobDir(rootPath, jobID));
      return false;
    }

    if (CheckpointingContext.startingFromACheckpoint(config) && jobZNodeExists) {
      // delete existing jobZnode
      JobZNodeManager.deleteJobZNode(client, rootPath, jobID);
    }

    // if jm is not restarting, create job directories
    JobZNodeManager.createJobZNode(client, rootPath, job);
    ZKEphemStateManager.createEphemDir(client, rootPath, job.getJobId());
    ZKEventsManager.createEventsZNode(client, rootPath, job.getJobId());
    ZKBarrierManager.createDefaultBarrierDir(client, rootPath, job.getJobId());
    ZKBarrierManager.createInitBarrierDir(client, rootPath, job.getJobId());
    ZKPersStateManager.createPersStateDir(client, rootPath, job.getJobId());

    long jsTime = Long.parseLong(System.getenv(K8sEnvVariables.JOB_SUBMISSION_TIME.name()));
    JobZNodeManager.createJstZNode(client, rootPath, jobID, jsTime);

    // create pers znode for jm
    ZKPersStateManager.createJobMasterPersState(client, rootPath, jobID, jmAddress);
    return true;
  }

}
