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

    Thread.sleep(20000);

    //gets the docker home directory
    //String homeDir = System.getenv("HOME");
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
      //container.execute(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      LOG.severe("Error " + e.getMessage());
    }

    //job master is the one with the id==0
    String jobMasterIP = workerNetworkInfoList.get(0).getWorkerIP().getHostAddress();
    LOG.info("JobMasterIP..: " + jobMasterIP);
    LOG.info("Worker ID..: " + workerId);
    StringBuilder outputBuilder = new StringBuilder();
    int workerCount = workerController.getNumberOfWorkers();
    LOG.info("Worker count..: " + workerCount);

    //start job master client
    worker.startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);

    //mpi master has the id equals to 1
    //id==0 is job master
    if (workerId == 1) {

      Writer writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream("/twister2/hostFile", true)));
      for (int i = 1; i < workerCount; i++) {
        writer.write(workerNetworkInfoList.get(i).getWorkerIP().getHostAddress()
            + "\n");
        LOG.info("Host IP..: " + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress());
      }
      writer.close();
      LOG.info("Before mpirun");
      //mpi command to run
      String[] command = {"mpirun", "-allow-run-as-root", "-npernode",
          "1", "--mca", "btl_tcp_if_include", "eth0",
          "--hostfile", "/twister2/hostFile", "java", "-cp",
          "twister2-job/libexamples-java.jar",
          "edu.iu.dsc.tws.examples.internal.rsched.BasicMpiJob", ">mpioutfile"};

      LOG.info("command:" + String.join(" ", command));
      //run the mpi command
      ProcessUtils.runSyncProcess(false, command, outputBuilder,
          new File("."), true);
      workerController.close();
      LOG.info("Job DONE");
    }

    jobMasterClient.sendWorkerCompletedMessage();
  }


  public void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    LOG.info("JobMasterIP..: " + jobMasterIP);

    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.startThreaded();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }


}
