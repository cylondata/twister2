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
package edu.iu.dsc.tws.rsched.core;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IJobMasterFailureListener;
import edu.iu.dsc.tws.api.resource.IReceiverFromDriver;
import edu.iu.dsc.tws.api.resource.IScalerListener;
import edu.iu.dsc.tws.api.resource.ISenderToDriver;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.common.zk.ZKWorkerController;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorkerController;

public final class WorkerRuntime {
  private static final Logger LOG = Logger.getLogger(WorkerRuntime.class.getName());

  private static boolean initialized = false;

  private static Config config;
  private static JobAPI.Job job;
  private static WorkerInfo workerInfo;

  private static ZKWorkerController zkWorkerController;
  private static JMWorkerAgent jmWorkerAgent;
  private static MPIWorkerController mpiWorkerController;
  private static boolean mpiWC = false;

  private static IWorkerController workerController;
  private static IWorkerStatusUpdater workerStatusUpdater;
  private static ISenderToDriver senderToDriver;

  private static IWorkerFailureListener failureListener;

  private WorkerRuntime() {
  }

  /**
   * Initialize WorkerRuntime with MPIWorkerController
   */
  public static synchronized boolean init(Config cnfg, MPIWorkerController wc) {
    if (initialized) {
      return false;
    }

    config = cnfg;
    mpiWC = true;
    mpiWorkerController = wc;

    workerController = mpiWorkerController;
    workerStatusUpdater = null;
    senderToDriver = null;

    initialized = true;
    return true;
  }

  /**
   * Initialize connections to Job Master or ZooKeeper
   */
  public static synchronized boolean init(Config cnfg,
                                          JobAPI.Job jb,
                                          WorkerInfo wInfo,
                                          int restartCount) {
    if (initialized) {
      return false;
    }

    config = cnfg;
    job = jb;
    workerInfo = wInfo;

    String jobMasterIP = JobMasterContext.jobMasterIP(config);

    // if the job is fault tolerant or uses ZK for group management
    // get IWorkerController and IWorkerStatusUpdater through ZKWorkerController
    if (ZKContext.isZooKeeperServerUsed(config)) {
      zkWorkerController =
          new ZKWorkerController(config, job.getJobId(), job.getNumberOfWorkers(), workerInfo);
      try {
        zkWorkerController.initialize(restartCount);
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
        throw new RuntimeException(e);
      }

      workerController = zkWorkerController;
      workerStatusUpdater = zkWorkerController;

      // if ZK is not used for group management, use JobMaster
    } else {

      // construct JMWorkerAgent
      jmWorkerAgent = JMWorkerAgent.createJMWorkerAgent(config, workerInfo, jobMasterIP,
          JobMasterContext.jobMasterPort(config), job.getNumberOfWorkers(), restartCount);

      // start JMWorkerAgent
      jmWorkerAgent.startThreaded();

      workerController = jmWorkerAgent.getJMWorkerController();
      workerStatusUpdater = jmWorkerAgent.getStatusUpdater();
      senderToDriver = jmWorkerAgent.getDriverAgent();
    }

    // if there is a driver in the job, we need to start JMWorkerAgent
    // if checkpointing is enabled, we need to start JMWorkerAgent
    // We only have one implementation for ISenderToDriver and CheckpointingClient
    // that is through JMWorkerAgent.
    if (ZKContext.isZooKeeperServerUsed(config)) {

      if (!job.getDriverClassName().isEmpty()
          || CheckpointingConfigurations.isCheckpointingEnabled(config)) {

        // construct JMWorkerAgent
        jmWorkerAgent = JMWorkerAgent.createJMWorkerAgent(config, workerInfo, jobMasterIP,
            JobMasterContext.jobMasterPort(config), job.getNumberOfWorkers(), restartCount);

        // start JMWorkerAgent
        jmWorkerAgent.startThreaded();

        // set checkpointing client from jmWorkerAgent to ZKWorkerController
        zkWorkerController.setCheckpointingClient(jmWorkerAgent.getCheckpointClient());

        // initialize JMSenderToDriver
        senderToDriver = jmWorkerAgent.getDriverAgent();

        // add listener to renew connection after jm restart
        if (FaultToleranceContext.faultTolerant(config)) {
          zkWorkerController.addJMFailureListener(new IJobMasterFailureListener() {
            @Override
            public void failed() {

            }

            @Override
            public void restarted(String jobMasterAddress) {
              LOG.info("JobMaster restarted. Worker will try to reconnect and re-register.");
              jmWorkerAgent.reconnect(jobMasterAddress);
            }
          });
        }
      }
    }

    initialized = true;
    return true;
  }

  /**
   * get IWorkerController
   */
  public static IWorkerController getWorkerController() {
    return workerController;
  }

  /**
   * get IWorkerStatusUpdater
   */
  public static IWorkerStatusUpdater getWorkerStatusUpdater() {
    return workerStatusUpdater;
  }

  /**
   * ISenderToDriver may be null, if there is no Driver in the job
   */
  public static synchronized ISenderToDriver getSenderToDriver() {
    return senderToDriver;
  }

  /**
   * add a IWorkerFailureListener
   * Currently failure notification is only implemented with ZKWorkerController
   * A listener can be only added when ZKWorkerController is used.
   */
  public static boolean addWorkerFailureListener(IWorkerFailureListener workerFailureListener) {
    failureListener = workerFailureListener;
    if (zkWorkerController != null) {
      return zkWorkerController.addFailureListener(workerFailureListener);
    } else {
      return jmWorkerAgent.addWorkerFailureListener(workerFailureListener);
    }
  }

  /**
   * Get the failure listener
   * @return the failure listener
   */
  public static IWorkerFailureListener getFailureListener() {
    return failureListener;
  }

  /**
   * Add IAllJoinedListener
   * It may return false, if a listener already added
   */
  public static boolean addAllJoinedListener(IAllJoinedListener allJoinedListener) {

    if (mpiWC) {
      mpiWorkerController.addAllJoinedListener(allJoinedListener);
      return true;
    }

    if (ZKContext.isZooKeeperServerUsed(config)) {
      return zkWorkerController.addAllJoinedListener(allJoinedListener);
    }

    if (jmWorkerAgent != null) {
      return JMWorkerAgent.addAllJoinedListener(allJoinedListener);
    }

    return false;
  }

  /**
   * Add IReceiverFromDriver
   * It may return false, if a listener already added or
   * if there is no Driver in the job
   */
  public static boolean addReceiverFromDriver(IReceiverFromDriver receiverFromDriver) {

    if (mpiWC) {
      return false;
    }

    if (job.getDriverClassName().isEmpty()) {
      return false;
    }

    if (jmWorkerAgent != null) {
      return JMWorkerAgent.addReceiverFromDriver(receiverFromDriver);
    }

    return false;
  }

  /**
   * Add IScalerListener
   * It may return false, if a listener already added or
   * if there is no Driver in the job
   */
  public static boolean addScalerListener(IScalerListener scalerListener) {

    if (mpiWC) {
      return false;
    }

    if (ZKContext.isZooKeeperServerUsed(config)) {
      return zkWorkerController.addScalerListener(scalerListener);
    }

    if (jmWorkerAgent != null) {
      return JMWorkerAgent.addScalerListener(scalerListener);
    }

    return false;
  }

  /**
   * add a IJobMasterFailureListener
   * Currently failure notification is only implemented with ZKWorkerController
   * A listener can be only added when ZKWorkerController is used.
   */
  public static boolean addJMFailureListener(IJobMasterFailureListener jobMasterListener) {
    if (zkWorkerController != null) {
      return zkWorkerController.addJMFailureListener(jobMasterListener);
    }

    return false;
  }

  public static void close() {
    // close the worker
    if (jmWorkerAgent != null) {
      jmWorkerAgent.close();
    }

    if (zkWorkerController != null) {
      zkWorkerController.close();
      ZKUtils.closeClient();
    }
  }

}
