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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static java.lang.Math.toIntExact;

public class MesosWorker implements Executor {

  public static final Logger LOG = Logger.getLogger(MesosWorker.class.getName());
  private static int executorCounter = 0;
  private Config config;
  private String jobName;

  @Override
  public void registered(ExecutorDriver executorDriver,
                         Protos.ExecutorInfo executorInfo,
                         Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
  }

  @Override
  public void reregistered(ExecutorDriver executorDriver,
                           Protos.SlaveInfo slaveInfo) {
  }

  @Override
  public void disconnected(ExecutorDriver executorDriver) {
  }

  @Override
  public void launchTask(ExecutorDriver executorDriver,
                         Protos.TaskInfo taskInfo) {

    LOG.info("Task start time(ms):" + System.currentTimeMillis());

    Integer id = Integer.parseInt(taskInfo.getData().toStringUtf8());
    LOG.info("Task " + id + " has started");
    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(Protos.TaskState.TASK_RUNNING).build();
    executorDriver.sendStatusUpdate(status);


    //jobName = SchedulerContext.jobName(config);
    //System.out.println("job name is " + jobName);
    String workerClass = SchedulerContext.workerClass(config);
    IWorker container;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      container = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
          workerClass), e);
      throw new RuntimeException(e);

    }
    long port = 0;
    for (Protos.Resource r : taskInfo.getResourcesList()) {
      if (r.getName().equals("ports")) {
        port = r.getRanges().getRange(0).getBegin();
        break;
      }

    }

    MesosWorkerController workerController;
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/" + jobName + ".job");
      workerController = new MesosWorkerController(config, job,
          InetAddress.getLocalHost().getHostAddress(), toIntExact(port), id);
      LOG.info("Initializing with zookeeper");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerController.waitForAllWorkersToJoin(ZKContext.maxWaitTimeForAllWorkersToJoin(config));
      LOG.info("Everyone has joined");
      container.execute(config, id, null, workerController, null, null);
      workerController.close();
    } catch (UnknownHostException e) {
      LOG.severe("Host unkown " + e.getMessage());
    }

    //The below two lines can be used to send a message to the framework
//    String reply = id.toString();
//    executorDriver.sendFrameworkMessage(reply.getBytes());

    LOG.info("Task " + id +  " has finished");
    status = Protos.TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(Protos.TaskState.TASK_FINISHED).build();
    executorDriver.sendStatusUpdate(status);
  }

  @Override
  public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
  }

  @Override
  public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {

  }

  @Override
  public void shutdown(ExecutorDriver executorDriver) {

  }

  @Override
  public void error(ExecutorDriver executorDriver, String s) {

  }

  public static void main(String[] args) throws Exception {

    MesosWorker worker = new MesosWorker();
    //worker.printArgs(args);

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    worker.config = ConfigLoader.loadConfig(twister2Home, configDir);

    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);


    worker.jobName = args[0];
    String workerName = args[1];
    initLogging(worker.config, SchedulerContext.nfsServerPath(worker.config)
            + "/" + worker.jobName + "/logs", workerName);

    System.out.println(worker.config);
    MesosExecutorDriver driver = new MesosExecutorDriver(
        worker);

    driver.run();
  }

  /**
   * Initialize the logger
   * @param cnfg
   * @param logDir
   * @param logFileName
   */
  public static void initLogging(Config cnfg, String logDir, String logFileName) {
    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cnfg));

    // if persistent logging is requested, initialize it
    if (LoggingContext.persistentLoggingRequested(cnfg)) {

      if (LoggingContext.redirectSysOutErr(cnfg)) {
        LOG.warning("Redirecting System.out and System.err to the log file. "
            + "Check the log file for the upcoming log messages. ");
      }

      LoggingHelper.setupLogging(cnfg, logDir, logFileName);

      LOG.info("Persistent logging to file initialized.");
    }
  }
}
