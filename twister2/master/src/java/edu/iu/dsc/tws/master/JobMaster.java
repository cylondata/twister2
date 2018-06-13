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
package edu.iu.dsc.tws.master;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.network.Network;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class JobMaster extends Thread {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  public static final int JOB_MASTER_ID = -1;
  private static Progress looper;

  private Config config;
  private String masterAddress;
  private int masterPort;
  private String jobName;

  private RRServer rrServer;
  private WorkerMonitor workerMonitor;
  private boolean workersCompleted = false;

  private IJobTerminator jobTerminator;

  public JobMaster(Config config,
                   String masterAddress,
                   IJobTerminator jobTerminator,
                   String jobName) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.jobTerminator = jobTerminator;
    this.jobName = jobName;
    this.masterPort = JobMasterContext.jobMasterPort(config);
  }

  public void init() {

    looper = new Progress();

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer =
        new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID, connectHandler);

    workerMonitor = new WorkerMonitor(config, this, rrServer);

    Network.Ping.Builder pingBuilder = Network.Ping.newBuilder();
    Network.WorkerStateChange.Builder stateChangeBuilder = Network.WorkerStateChange.newBuilder();
    Network.WorkerStateChangeResponse.Builder stateChangeResponseBuilder
        = Network.WorkerStateChangeResponse.newBuilder();

    ListWorkersRequest.Builder listWorkersBuilder = ListWorkersRequest.newBuilder();
    ListWorkersResponse.Builder listResponseBuilder = ListWorkersResponse.newBuilder();

    rrServer.registerRequestHandler(pingBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(listWorkersBuilder, workerMonitor);
    rrServer.registerRequestHandler(listResponseBuilder, workerMonitor);

    rrServer.start();
    looper.loop();
    start();
  }

  @Override
  public void run() {
    LOG.info("JobMaster [" + masterAddress + "] started and waiting worker messages on port: "
        + masterPort);

    while (!workersCompleted) {
      looper.loopBlocking();
    }

    // to send the last remaining messages if any
    looper.loop();
    looper.loop();
    looper.loop();

    rrServer.stop();
  }

  /**
   * this method is executed when the worker completed message received from all workers
   */
  public void allWorkersCompleted() {

    LOG.info("All workers have completed. JobMaster will stop.");
    workersCompleted = true;
    looper.wakeup();

    if (jobTerminator != null) {
      jobTerminator.terminateJob(jobName);
    }
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      try {
        LOG.finer("Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    Config envConfigs = buildConfigFromEnvVariables();

    initLogger(envConfigs);

    LOG.info("JobMaster started. Current time: " + System.currentTimeMillis());
    LOG.info("Received parameters as environment variables: \n" + envConfigs);

    String host = JobMasterContext.jobMasterIP(envConfigs);
    String jobName = Context.jobName(envConfigs);

    JobMaster jobMaster = new JobMaster(envConfigs, host, null, jobName);
    jobMaster.init();

  }


  /**
   * construct a Config object from environment variables
   * @return
   */
  public static Config buildConfigFromEnvVariables() {
    return Config.newBuilder()
        .put(JobMasterContext.JOB_MASTER_IP, System.getenv(JobMasterContext.JOB_MASTER_IP))
        .put(JobMasterContext.JOB_MASTER_PORT, System.getenv(JobMasterContext.JOB_MASTER_PORT))
        .put(Context.JOB_NAME, System.getenv(Context.JOB_NAME))
        .put(Context.TWISTER2_WORKER_INSTANCES, System.getenv(Context.TWISTER2_WORKER_INSTANCES))
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS,
            System.getenv(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS))
        .put(JobMasterContext.PERSISTENT_JOB_DIRECTORY,
            System.getenv(JobMasterContext.PERSISTENT_JOB_DIRECTORY))
        .put(JobMasterContext.PING_INTERVAL, System.getenv(JobMasterContext.PING_INTERVAL))
        .put(LoggingContext.PERSISTENT_LOGGING_REQUESTED,
            System.getenv(LoggingContext.PERSISTENT_LOGGING_REQUESTED))
        .put(LoggingContext.LOGGING_LEVEL, System.getenv(LoggingContext.LOGGING_LEVEL))
        .put(LoggingContext.REDIRECT_SYS_OUT_ERR,
            System.getenv(LoggingContext.REDIRECT_SYS_OUT_ERR))
        .put(LoggingContext.MAX_LOG_FILE_SIZE, System.getenv(LoggingContext.MAX_LOG_FILE_SIZE))
        .put(LoggingContext.MAX_LOG_FILES, System.getenv(LoggingContext.MAX_LOG_FILES))
        .build();
  }

  /**
   * itinialize the logger
   * @param cnfg
   */
  public static void initLogger(Config cnfg) {
    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    String persistentJobDir = JobMasterContext.persistentJobDirectory(cnfg);
    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }

    // if persistent logging is requested, initialize it
    if (LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      String persistentLogDir = persistentJobDir + "/logs";
      String logFileName = "jobMaster";

      LoggingHelper.setupLogging(cnfg, persistentLogDir, logFileName);

      LOG.info("Persistent logging to file initialized.");
    }
  }



}
