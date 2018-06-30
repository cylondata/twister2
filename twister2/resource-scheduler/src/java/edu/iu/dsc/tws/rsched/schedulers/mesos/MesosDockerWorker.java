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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Inet4Address;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;


public class MesosDockerWorker {

  public static final Logger LOG = Logger.getLogger(MesosDockerWorker.class.getName());
  private Config config;
  private String jobName;


 /*
  public void launchTask(ExecutorDriver executorDriver,
                         Protos.TaskInfo taskInfo) {

    Integer id = Integer.parseInt(taskInfo.getData().toStringUtf8());
    LOG.info("Task " + id + " has started");
    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(Protos.TaskState.TASK_RUNNING).build();
    executorDriver.sendStatusUpdate(status);


    //jobName = SchedulerContext.jobName(config);
    //System.out.println("job name is " + jobName);
    String containerClass = SchedulerContext.containerClass(config);
    IWorker container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IWorker) object;
      LOG.info("loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
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
      container.init(config, id, null, workerController, null);
      workerController.close();
    } catch (UnknownHostException e) {
      e.printStackTrace();
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
  */

  public static void main(String[] args) throws Exception {


    String hostIp = System.getenv("HOST_IP");
    String homeDir = System.getenv("HOME");
    int sshPort = Integer.parseInt(System.getenv("SSH_PORT"));
    int workerId = Integer.parseInt(System.getenv("WORKER_ID"));
    String jobName = System.getenv("JOB_NAME");
    System.out.println("host ip:" + hostIp);
    System.out.println("SSH_PORT: " + sshPort);

    int id = workerId;
    MesosDockerWorker worker = new MesosDockerWorker();
    //worker.printArgs(args);

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    worker.config = ConfigLoader.loadConfig(twister2Home, configDir);
    worker.jobName = jobName;

    MesosWorkerLogger logger = new MesosWorkerLogger(worker.config,
        "/persistent-volume/logs", "worker" + workerId);
    logger.initLogging();
    String containerClass = SchedulerContext.containerClass(worker.config);
    IWorker container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IWorker) object;
      LOG.info("loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
      throw new RuntimeException(e);

    }


    MesosWorkerController workerController;
    List<WorkerNetworkInfo> workerNetworkInfoList = new ArrayList<>();
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");
      workerController = new MesosWorkerController(worker.config, job,
          Inet4Address.getLocalHost().getHostAddress(), 22, id);
      LOG.info("Initializing with zookeeper");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerNetworkInfoList = workerController.waitForAllWorkersToJoin(
          ZKContext.maxWaitTimeForAllWorkersToJoin(worker.config));
      LOG.info("Everyone has joined");
      //container.init(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      e.printStackTrace();


      System.out.println("inside catch");
      LOG.severe("Inside catch!!!!!!!!!");
      //to eleminate workerController might not have been initialized error
      //it's copied for testing. Don't forget to delete
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");
      workerController = new MesosWorkerController(worker.config, job,
          Inet4Address.getLocalHost().getHostAddress(), 22, id);

    }


    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    //LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    System.out.println(worker.config);
    //worker.jobName = args[0];
    //String workerName = args[1];
    //initLogging(worker.config, SchedulerContext.nfsServerPath(worker.config)
    //    + "/" + worker.jobName + "/logs", workerName);

    System.out.println("Worker id " + id);
    StringBuilder outputBuilder = new StringBuilder();


    System.out.println("inside if");
    LOG.info("inside if");


    int workerCount = workerController.getNumberOfWorkers();
    System.out.println("worker count " + workerCount);


    //System.setProperty("file.encoding", "Cp1252");

    //PrintWriter writer = new PrintWriter("hosts.txt", "UTF-8");


      /*String[] command = {"mpirun", "-allow-run-as-root", "-np",
          workerController.getNumberOfWorkers() + "", "-hosts", hosts.substring(0,
          hosts.lastIndexOf(',')), "java", "-cp",
          "\"twister2-core/lib/*:twister2-job/libexamples-java.jar\"",
          "edu.iu.dsc.tws.examples.basic.BasicMpiJob"}; */


    if (id == 0) {






      File file = new File(homeDir + "/.ssh/config");
      boolean b = file.getParentFile().mkdirs();
      if (b) {
        System.out.println("file created succesfully");
      } else {
        //System.out.println("!!!!!!!something wrong!!!!!");
      }

      Writer writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(homeDir + "/.ssh/config", true)));

      writer.write("Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null\n"
          + "\tIdentityFile ~/.ssh/id_rsa\n");


      //ProcessUtils.runProcess("touch hosts.txt", new StringBuilder(), true);
      String hosts = "";

      for (int i = 0; i < workerCount; i++) {

        /*writer.write(workerNetworkInfoList.get(i).getWorkerIP().getHostAddress()
            + " port=" + workerNetworkInfoList.get(i).getWorkerPort() + "\n");
        */

        writer.write("Host w" + workerNetworkInfoList.get(i).getWorkerID() + "\n"
            + "\tHostname " + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress() + "\n"
            + "\tPort " + workerNetworkInfoList.get(i).getWorkerPort() + "\n");

        System.out.println("Host w" + workerNetworkInfoList.get(i).getWorkerID() + "\n"
            + "\tHostname " + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress() + "\n"
            + "\tPort " + workerNetworkInfoList.get(i).getWorkerPort() + "\n");


        hosts += "w" + workerNetworkInfoList.get(i).getWorkerID() + ",";


      }


      writer.close();
      hosts = hosts.substring(0, hosts.lastIndexOf(','));







      System.out.println("Before mpirun");
      System.out.println("hosts " + hosts);
      String[] command = {"mpirun", "-allow-run-as-root", "-np",
          workerController.getNumberOfWorkers() + "",
          "--host", hosts, "java", "-cp",
          "twister2-job/libexamples-java.jar",
          "edu.iu.dsc.tws.examples.basic.BasicMpiJob", ">mpioutfile"};

      System.out.println("command:" + String.join(" ", command));

      ProcessUtils.runSyncProcess(false, command, outputBuilder,
          new File("."), true);
      workerController.close();
      System.out.println("Finished");
    }


  }


}
