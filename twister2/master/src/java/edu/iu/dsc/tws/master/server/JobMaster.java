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

package edu.iu.dsc.tws.master.server;

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.driver.DefaultDriver;
import edu.iu.dsc.tws.api.driver.IDriver;
import edu.iu.dsc.tws.api.driver.IScalerPerCluster;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.net.StatusCode;
import edu.iu.dsc.tws.api.net.request.ConnectHandler;
import edu.iu.dsc.tws.checkpointing.master.CheckpointManager;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.zk.JobZNodeManager;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.barrier.BarrierMonitor;
import edu.iu.dsc.tws.master.barrier.JMBarrierHandler;
import edu.iu.dsc.tws.master.barrier.ZKBarrierHandler;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.master.driver.DriverMessenger;
import edu.iu.dsc.tws.master.driver.Scaler;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * JobMaster class
 * It is started for each Twister2 job
 * It provides:
 * worker discovery
 * barrier method
 * Ping service
 * <p>
 * It can be started in two different modes:
 * Threaded and Blocking
 * <p>
 * If the user calls:
 * startJobMasterThreaded()
 * It starts as a Thread and the call to this method returns
 * <p>
 * If the user calls:
 * startJobMasterBlocking()
 * It uses the calling thread and this call does not return unless the JobMaster completes
 * <p>
 * JobMaster to Dashboard messaging
 * JobMaster reports to Dashboard server when dashboard address is provided in the config
 * If dashboard host address is not provided, it does not try to connect to dashboard server
 */

public class JobMaster {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  /**
   * an id to be used when comminicating with workers and the client
   */
  public static final int JOB_MASTER_ID = -10;

  /**
   * A singleton Progress object monitors network channel
   */
  private static Progress looper;

  /**
   * config object for the Job Master
   */
  private Config config;

  /**
   * the ip address of this job master
   */
  private String jmAddress;

  /**
   * port number of this job master
   */
  private int masterPort;

  /**
   * name of the job this Job Master will manage
   */
  private JobAPI.Job job;

  /**
   * the network object to receive and send messages
   */
  private RRServer rrServer;

  /**
   * the object to monitor workers
   */
  private WorkerMonitor workerMonitor;

  /**
   * a flag to show that whether the job is ended
   * when it is set to true, the job master finishes the execution
   */
  private boolean jobEnded = false;

  /**
   * a flag to show that whether the job master failed
   */
  private boolean jobMasterFailed = false;

  /**
   * final state of the job
   * it is set when the job has been ended
   */
  private JobAPI.JobState finalState;

  /**
   * Job Terminator object.
   * it will the terminate all workers and cleanup job resources.
   */
  private IJobTerminator jobTerminator;

  /**
   * NodeInfo object for Job Master
   * location of Job Master
   */
  private JobMasterAPI.NodeInfo nodeInfo;

  /**
   * the scaler for the cluster in that Job Master is running
   */
  private IScalerPerCluster clusterScaler;

  /**
   * the driver object
   */
  private IDriver driver;

  /**
   * host address of Dashboard server
   * if it is set, job master will report to Dashboard
   * otherwise, it will ignore Dashboard
   */
  private String dashboardHost;

  /**
   * the client that will handle Dashboard messaging
   */
  private DashboardClient dashClient;

  /**
   * WorkerHandler object to communicate with workers
   */
  private JMWorkerHandler workerHandler;

  private BarrierMonitor barrierMonitor;

  /**
   * to connect to ZooKeeper server and update job object
   * in case of scaling or job state changes
   */
  private ZKJobUpdater zkJobUpdater;

  /**
   * JobMaster to ZooKeeper connection
   */
  private ZKMasterController zkMasterController;

  /**
   * barrier handler for zk
   */
  private ZKBarrierHandler zkBarrierHandler;

  /**
   * a variable that shows whether JobMaster will run jobTerminate
   * when it is killed with a shutdown hook
   */
  private boolean clearK8sResourcesWhenKilled;

  private CheckpointManager checkpointManager;

  private JobMasterAPI.JobMasterState initialState;

  /**
   * max back log value for tcp connections in Job Master
   * it is set as numberOfWorkers/2
   * if numberOfWorkers/2 < 50, backLog is not set. the Default is used.
   * if numberOfWorkers/2 > 2048, backLog is set to 2048.
   */
  private static final int MAX_BACK_LOG = 2048;

  /**
   * JobMaster constructor
   *
   * @param config configuration
   * @param jmAddress master host
   * @param port the port number
   * @param jobTerminator terminator
   * @param job the job in proto format
   * @param nodeInfo node info of master
   */
  public JobMaster(Config config,
                   String jmAddress,
                   int port,
                   IJobTerminator jobTerminator,
                   JobAPI.Job job,
                   JobMasterAPI.NodeInfo nodeInfo,
                   IScalerPerCluster clusterScaler,
                   JobMasterAPI.JobMasterState initialState) {
    this.config = config;
    this.jmAddress = jmAddress;
    this.jobTerminator = jobTerminator;
    this.job = job;
    this.nodeInfo = nodeInfo;
    this.masterPort = port;
    this.clusterScaler = clusterScaler;
    this.initialState = initialState;

    this.zkJobUpdater = new ZKJobUpdater(config, job.getJobId());

    this.dashboardHost = JobMasterContext.dashboardHost(config);
    if (dashboardHost == null) {
      LOG.warning("Dashboard host address is null. Not connecting to Dashboard");
      this.dashClient = null;
    } else {
      this.dashClient = new DashboardClient(
          dashboardHost, job.getJobId(), JobMasterContext.jmToDashboardConnections(config));
    }
  }

  /**
   * JobMaster constructor to create a job master, the port of job master is read from config
   * file
   *
   * @param config configuration
   * @param jmAddress master host
   * @param jobTerminator terminator
   * @param job the job in proto format
   * @param nodeInfo node info of master
   */
  public JobMaster(Config config,
                   String jmAddress,
                   IJobTerminator jobTerminator,
                   JobAPI.Job job,
                   JobMasterAPI.NodeInfo nodeInfo,
                   IScalerPerCluster clusterScaler,
                   JobMasterAPI.JobMasterState initialState) {

    this(config, jmAddress, JobMasterContext.jobMasterPort(config),
        jobTerminator, job, nodeInfo, clusterScaler, initialState);
  }


  /**
   * initialize the Job Master
   */
  private void init() throws Twister2Exception {

    looper = new Progress();

    // if Dashboard is used, register this job with that
    if (dashClient != null) {
      boolean registered = dashClient.registerJob(job, nodeInfo);
      if (!registered) {
        LOG.warning("Not using Dashboard since it can not register with it.");
        dashClient = null;
      }
    }

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    int backLog = Math.min(job.getNumberOfWorkers() / 2, MAX_BACK_LOG);
    rrServer =
        new RRServer(config, jmAddress, masterPort, looper, JOB_MASTER_ID, connectHandler, backLog);

    // init Driver if it exists
    // this ha to be done before WorkerMonitor initialization
    initDriver();

    JobFailureWatcher jobFailureWatcher = new JobFailureWatcher();

    workerMonitor = new WorkerMonitor(
        this, rrServer, dashClient, zkJobUpdater, job, driver, jobFailureWatcher);

    workerHandler =
        new JMWorkerHandler(workerMonitor, rrServer, ZKContext.isZooKeeperServerUsed(config));
    if (!ZKContext.isZooKeeperServerUsed(config)) {
      workerMonitor.setWorkerEventSender(workerHandler);
    }

    // initialize BarrierMonitor
    barrierMonitor = new BarrierMonitor(workerMonitor, jobFailureWatcher);
    if (ZKContext.isZooKeeperServerUsed(config)) {
      zkBarrierHandler =
          new ZKBarrierHandler(barrierMonitor, config, job.getJobId(), job.getNumberOfWorkers());
      barrierMonitor.setBarrierResponder(zkBarrierHandler);
      zkBarrierHandler.initialize(initialState);
    } else {
      JMBarrierHandler jmBarrierHandler = new JMBarrierHandler(rrServer, barrierMonitor);
      barrierMonitor.setBarrierResponder(jmBarrierHandler);
    }
    jobFailureWatcher.addJobFaultListener(barrierMonitor);

    // if ZoKeeper server is used for this job, initialize that
    try {
      initZKMasterController(workerMonitor);
    } catch (Twister2Exception e) {
      throw e;
    }

    //initialize checkpoint manager
    if (CheckpointingContext.isCheckpointingEnabled(config)) {
      StateStore stateStore = CheckpointUtils.getStateStore(config);
      stateStore.init(config, job.getJobId());
      this.checkpointManager = new CheckpointManager(
          this.rrServer,
          stateStore,
          job.getJobId()
      );
      jobFailureWatcher.addJobFaultListener(this.checkpointManager);
      LOG.info("Checkpoint manager initialized");
      this.checkpointManager.init();
    }
    //done initializing checkpoint manager

    rrServer.start();
    looper.loop();
  }

  /**
   * start the Job Master in a Thread
   */
  public Thread startJobMasterThreaded() throws Twister2Exception {
    // first call the init method
    try {
      init();
    } catch (Twister2Exception e) {
      throw e;
    }

    // start Driver thread if the driver exists
    startDriverThread();

    Thread jmThread = new Thread() {
      public void run() {
        startLooping();
      }
    };

    jmThread.setName("JM");
    jmThread.setDaemon(true);
    jmThread.start();

    return jmThread;
  }

  /**
   * start the Job Master in a blocking call
   */
  public void startJobMasterBlocking() throws Twister2Exception {
    // first call the init method
    try {
      init();
    } catch (Twister2Exception e) {
      throw e;
    }

    // start Driver thread if the driver exists

    startDriverThread();

    startLooping();
  }

  /**
   * Job Master loops until all workers in the job completes
   */
  private void startLooping() {
    LOG.info("JobMaster [" + jmAddress + "] started and waiting worker messages on port: "
        + masterPort);

    while (!jobEnded && !jobMasterFailed) {
      looper.loopBlocking(300);

      // check for barrier failures periodically
      barrierMonitor.checkBarrierFailure();
    }

    if (jobMasterFailed) {
      return;
    }

    close();
  }

  /**
   * called when the job has ended, or the job master failed
   */
  private void close() {
    // send the remaining messages if any and stop
    rrServer.stopGraceFully(2000);

    // if the job has completed successfully, killed or failed,
    // we need to cleanup
    if (ZKContext.isZooKeeperServerUsed(config)) {

      if (jobEnded) {
        JobZNodeManager.createJobEndTimeZNode(
            ZKUtils.getClient(), ZKContext.rootNode(config), job.getJobId());
        ZKPersStateManager.updateJobMasterStatus(ZKUtils.getClient(), ZKContext.rootNode(config),
            job.getJobId(), jmAddress, JobMasterAPI.JobMasterState.JM_COMPLETED);
      } else if (jobMasterFailed) {
        ZKPersStateManager.updateJobMasterStatus(ZKUtils.getClient(), ZKContext.rootNode(config),
            job.getJobId(), jmAddress, JobMasterAPI.JobMasterState.JM_FAILED);
      }

      zkMasterController.close();
      zkBarrierHandler.close();
      ZKUtils.closeClient();
    }

    if (jobEnded && jobTerminator != null) {
      jobTerminator.terminateJob(job.getJobId(), finalState);
    }

    if (dashClient != null) {
      dashClient.close();
    }
  }

  private void initDriver() {

    //If the job master is running on the client set the driver to default driver.
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      driver = new DefaultDriver();
    }
    // if Driver is not set, can not initialize Driver
    if (job.getDriverClassName().isEmpty()) {
      return;
    }

    // first construct the driver
    String driverClass = job.getDriverClassName();
    try {
      Object object = ReflectionUtils.newInstance(driverClass);
      driver = (IDriver) object;
      LOG.info("loaded driver class: " + driverClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the driver class %s", driverClass));
      throw new RuntimeException(e);
    }
  }

  /**
   * start Driver in a Thread
   */
  public Thread startDriverThread() {

    if (driver == null) {
      return null;
    }

    Thread driverThread = new Thread() {
      public void run() {
        Scaler scaler = new Scaler(clusterScaler, workerMonitor, zkJobUpdater);
        DriverMessenger driverMessenger = new DriverMessenger(workerMonitor);
        driver.execute(config, scaler, driverMessenger);
      }
    };
    driverThread.setName("driver");
    driverThread.start();

    // if all workers already joined, publish that event to the driver
    // this usually happens when jm restarted
    // since now we require all workers to be both joined and connected,
    // this should not be an issue, but i am not %100 sure. so keeping it.
    // TODO: make sure driver thread started before publishing this event
    //       as a temporary solution, wait 50 ms before starting new thread
    if (workerMonitor.isAllJoined()) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        LOG.warning("Thread sleep interrupted.");
      }

      new Thread("Twister2-AllJoinedSupplierToDriver") {
        @Override
        public void run() {
          workerMonitor.informDriverForAllJoined();
        }
      }.start();
    }

    return driverThread;
  }

  /**
   * initialize ZKMasterController if ZooKeeper used
   */
  private void initZKMasterController(WorkerMonitor wMonitor)
      throws Twister2Exception {

    if (ZKContext.isZooKeeperServerUsed(config)) {
      zkMasterController = new ZKMasterController(
          config, job.getJobId(), job.getNumberOfWorkers(), jmAddress, wMonitor);
      workerMonitor.setWorkerEventSender(zkMasterController);

      try {
        zkMasterController.initialize(initialState);
      } catch (Twister2Exception e) {
        throw e;
      }
    }

  }

  public ZKMasterController getZkMasterController() {
    return zkMasterController;
  }

  public JMWorkerHandler getWorkerHandler() {
    return workerHandler;
  }

  /**
   * this method is called when JobMaster fails because of some uncaught exception
   * ıt is called by the JobMasterStarter program
   *
   * It closes all threads started by JM
   * It marks its state at ZK persistent storage as FAILED
   */
  public void jmFailed() {
    jobMasterFailed = true;
    close();
  }

  /**
   * this method finishes the job
   * It is executed when the worker completed message received from all workers
   */
  public void endJob(JobAPI.JobState finalState1) {
    jobEnded = true;
    this.finalState = finalState1;
    looper.wakeup();
  }

  /**
   * A job can be terminated in two ways:
   * a) workers end a job: workers either successfully complete or fail
   *   all workers complete their work and send COMPLETED messages to the Job Master.
   *   some workers send FULLY_FAILED message to JM
   * Job master clears all job resources from the cluster and informs Dashboard.
   * This is handled in the method: endJob()
   * b) client kills the job: users kill the job explicitly by either
   * executing job kill command or killing the client process if JM is running on the  client.
   * JobMaster endJob() method is not called.
   * JM ShutDown Hook gets executed.
   * <p>
   * In this case, it can either clear job resources on k8s or does not
   * based on the parameter that is provided when the shut down hook is registered.
   * If the job master runs in the client, it should clear k8s resources,
   * because the job is ended by killing JM process.
   * when the job master runs in the cluster, it should not clear resources.
   * The resources should be cleared by the twister2 client.
   */
  public void addShutdownHook(boolean clearK8sJobResourcesOnKill) {
    clearK8sResourcesWhenKilled = clearK8sJobResourcesOnKill;

    Thread hookThread = new Thread() {
      public void run() {

        // if the job is ended, do nothing
        if (jobEnded || jobMasterFailed) {
          return;
        }

        // if the job is not ended, it must have been killed
        finalState = JobAPI.JobState.KILLED;

        if (ZKContext.isZooKeeperServerUsed(config)) {
          zkJobUpdater.updateState(finalState);
          JobZNodeManager.createJobEndTimeZNode(
              ZKUtils.getClient(), ZKContext.rootNode(config), job.getJobId());
          ZKPersStateManager.updateJobMasterStatus(ZKUtils.getClient(), ZKContext.rootNode(config),
              job.getJobId(), jmAddress, JobMasterAPI.JobMasterState.JM_KILLED);
          zkMasterController.close();
          zkBarrierHandler.close();
          ZKUtils.closeClient();
        }

        // if Dashboard is used, tell it that the job is killed
        if (dashClient != null) {
          dashClient.jobStateChange(finalState);
        }

        // let the main jm thread know that the job has been ended by killing
        jobEnded = true;
        looper.wakeup();

        if (clearK8sResourcesWhenKilled) {
          if (jobTerminator != null) {
            jobTerminator.terminateJob(job.getJobId(), finalState);
          }
        }

      }
    };

    Runtime.getRuntime().addShutdownHook(hookThread);
  }

  public IDriver getDriver() {
    return driver;
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel, StatusCode status) {
      LOG.warning("Error on channel: " + channel.socket().getRemoteSocketAddress());
    }

    @Override
    public void onConnect(SocketChannel channel) {
      LOG.fine("Client connected from: " + channel.socket().getRemoteSocketAddress());
    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.fine("Client closed: " + channel.socket().getRemoteSocketAddress());
    }
  }

}
