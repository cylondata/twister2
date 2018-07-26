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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;


public class MesosDockerWorker {

  public static final Logger LOG = Logger.getLogger(MesosDockerWorker.class.getName());
  public static JobMasterClient jobMasterClient;
  private Config config;
  private String jobName;

  public static void main(String[] args) throws Exception {


    Thread.sleep(5000);
    //gets the docker home directory
    String homeDir = System.getenv("HOME");
    int workerId = Integer.parseInt(System.getenv("WORKER_ID"));
    String jobName = System.getenv("JOB_NAME");
    MesosDockerWorker worker = new MesosDockerWorker();

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    worker.config = ConfigLoader.loadConfig(twister2Home, configDir);
    worker.jobName = jobName;

    MesosWorkerLogger logger = new MesosWorkerLogger(worker.config,
        "/persistent-volume/logs", "worker" + workerId);
    logger.initLogging();


    /*
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
    */


    MesosWorkerController workerController = null;
    List<WorkerNetworkInfo> workerNetworkInfoList = new ArrayList<>();
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");
      workerController = new MesosWorkerController(worker.config, job,
          Inet4Address.getLocalHost().getHostAddress(), 2022, workerId);
      LOG.info("Initializing with zookeeper");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerNetworkInfoList = workerController.waitForAllWorkersToJoin(
          ZKContext.maxWaitTimeForAllWorkersToJoin(worker.config));
      LOG.info("Everyone has joined");
      //container.init(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      e.printStackTrace();
    }

    String jobMasterIP = workerNetworkInfoList.get(0).getWorkerIP().getHostAddress();
    LOG.info("JobMasterIP" + jobMasterIP);
    System.out.println("Worker id " + workerId);
    StringBuilder outputBuilder = new StringBuilder();
    int workerCount = workerController.getNumberOfWorkers();
    System.out.println("worker count " + workerCount);
    worker.startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);




    //mpi master has the id equals to 1
    //id==0 is job master
    if (workerId == 1) {

      Writer writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream("/twister2/hostFile", true)));

      for (int i = 1; i < workerCount; i++) {

        writer.write(workerNetworkInfoList.get(i).getWorkerIP().getHostAddress() + "\n");

        System.out.println("host ip: "
            + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress());
      }

      writer.close();
      System.out.println("Before mpirun");
      String[] command = {"mpirun", "-allow-run-as-root", "-np",
          (workerController.getNumberOfWorkers() - 1) + "",
          "--hostfile", "/twister2/hostFile", "java", "-cp",
          "twister2-job/libexamples-java.jar",
          "edu.iu.dsc.tws.examples.basic.BasicMpiJob", ">mpioutfile"};

      System.out.println("command:" + String.join(" ", command));

      ProcessUtils.runSyncProcess(false, command, outputBuilder,
          new File("."), true);
      workerController.close();
      System.out.println("Finished");
    }


    Thread.sleep(20000);
    jobMasterClient.sendWorkerCompletedMessage();
  }


  public void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    //String jobMasterIP = JobMasterContext.jobMasterIP(config);
    // jobMasterIP = jobMasterIP.trim();

    // if jobMasterIP is null, or the length zero,
    // job master runs as a separate pod
    // get its IP address first
    /*if (jobMasterIP == null || jobMasterIP.length() == 0) {

      DiscoverJobMaster djm = new DiscoverJobMaster();
      jobMasterIP = djm.waitUntilJobMasterRunning(jobMasterPodName, 10000);

      cnf = Config.newBuilder()
          .putAll(cnfg)
          .put(JobMasterContext.JOB_MASTER_IP, jobMasterIP)
          .build();
    }
    */

    LOG.info("JobMasterIP: " + jobMasterIP);

    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.init();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }


}
