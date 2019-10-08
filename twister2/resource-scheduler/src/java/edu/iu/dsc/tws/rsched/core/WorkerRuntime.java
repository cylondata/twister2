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
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IJobMasterListener;
import edu.iu.dsc.tws.api.resource.IReceiverFromDriver;
import edu.iu.dsc.tws.api.resource.IScalerListener;
import edu.iu.dsc.tws.api.resource.ISenderToDriver;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKWorkerController;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.worker.JMSenderToDriver;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.master.worker.JMWorkerStatusUpdater;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class WorkerRuntime {
  private static final Logger LOG = Logger.getLogger(WorkerRuntime.class.getName());

  private static WorkerRuntime workerRuntime;

  private static Config config;
  private static JobAPI.Job job;
  private static WorkerInfo workerInfo;

  private static ZKWorkerController zkWorkerController;
  private static JMWorkerAgent jmWorkerAgent;

  private static IWorkerController workerController;
  private static IWorkerStatusUpdater workerStatusUpdater;
  private static ISenderToDriver senderToDriver;

  private WorkerRuntime() {
  }

  /**
   * Initialize connections to Job Master or ZooKeeper
   * @param cnfg
   * @param jb
   * @param wInfo
   * @return
   */
  public static synchronized WorkerRuntime init(Config cnfg,
                                                JobAPI.Job jb,
                                                WorkerInfo wInfo,
                                                JobMasterAPI.WorkerState initialState) {
    if (workerRuntime != null) {
      return workerRuntime;
    }

    config = cnfg;
    job = jb;
    workerInfo = wInfo;

    String jobMasterIP = JobMasterContext.jobMasterIP(config);

    // if there is a driver in the job, we need to start JMWorkerAgent
    // We only have one implementation for ISenderToDriver that is through JMWorkerAgent.
    if (!job.getDriverClassName().isEmpty()) {
      // construct JMWorkerAgent
      jmWorkerAgent = JMWorkerAgent.createJMWorkerAgent(config, workerInfo, jobMasterIP,
          JobMasterContext.jobMasterPort(config), job.getNumberOfWorkers());

      // start JMWorkerAgent
      jmWorkerAgent.startThreaded();

      // initialize JMSenderToDriver
      senderToDriver = new JMSenderToDriver(jmWorkerAgent);
    }

    // if the job is fault tolerant or uses ZK for group management
    // get IWorkerController and IWorkerStatusUpdater through ZKWorkerController
    if (ZKContext.isZooKeeperServerUsed(config)) {
      zkWorkerController =
          new ZKWorkerController(config, job.getJobName(), job.getNumberOfWorkers(), workerInfo);
      try {
        zkWorkerController.initialize(initialState);
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
        throw new RuntimeException(e);
      }

      workerController = zkWorkerController;
      workerStatusUpdater = zkWorkerController;

      // if ZK is not used for group management, use JobMaster
    } else {

      // if jobMasterAgent has not already been initialized, start it
      if (jmWorkerAgent == null) {

        // construct JMWorkerAgent
        jmWorkerAgent = JMWorkerAgent.createJMWorkerAgent(config, workerInfo, jobMasterIP,
            JobMasterContext.jobMasterPort(config), job.getNumberOfWorkers());

        // start JMWorkerAgent
        jmWorkerAgent.startThreaded();
      }

      workerController = jmWorkerAgent.getJMWorkerController();
      workerStatusUpdater = new JMWorkerStatusUpdater(jmWorkerAgent);
    }

    workerRuntime = new WorkerRuntime();
    return workerRuntime;
  }

  /**
   * get IWorkerController
   * @return
   */
  public static IWorkerController getWorkerController() {
    return workerController;
  }

  /**
   * get IWorkerStatusUpdater
   * @return
   */
  public static IWorkerStatusUpdater getWorkerStatusUpdater() {
    return workerStatusUpdater;
  }

  /**
   * ISenderToDriver may be null, if there is no Driver in the job
   * @return
   */
  public static synchronized ISenderToDriver getSenderToDriver() {
    return senderToDriver;
  }

  /**
   * add a IWorkerFailureListener
   * Currently failure notification is only implemented with ZKWorkerController
   * A listener can be only added when ZKWorkerController is used.
   * @param workerFailureListener
   * @return
   */
  public static boolean addWorkerFailureListener(IWorkerFailureListener workerFailureListener) {
    if (zkWorkerController != null) {
      return zkWorkerController.addFailureListener(workerFailureListener);
    }

    return false;
  }

  /**
   * Add IAllJoinedListener
   * It may return false, if a listener already added
   * @param allJoinedListener
   * @return
   */
  public static boolean addAllJoinedListener(IAllJoinedListener allJoinedListener) {

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
   * @param receiverFromDriver
   * @return
   */
  public static boolean addReceiverFromDriver(IReceiverFromDriver receiverFromDriver) {

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
   * @param scalerListener
   * @return
   */
  public static boolean addScalerListener(IScalerListener scalerListener) {

    if (job.getDriverClassName().isEmpty()) {
      return false;
    }

    if (jmWorkerAgent != null) {
      return JMWorkerAgent.addScalerListener(scalerListener);
    }

    return false;
  }

  /**
   * add a IJobMasterListener
   * Currently failure notification is only implemented with ZKWorkerController
   * A listener can be only added when ZKWorkerController is used.
   * @param jobMasterListener
   * @return
   */
  public static boolean addJobMasterListener(IJobMasterListener jobMasterListener) {
    if (zkWorkerController != null) {
      return zkWorkerController.addJobMasterListener(jobMasterListener);
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
    }
  }

}
