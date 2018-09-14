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
package edu.iu.dsc.tws.rsched.schedulers.mesos.mpi;

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
import edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterFinder;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerController;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerLogger;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

public final class MesosMPIMasterStarter {

  public static final Logger LOG = Logger.getLogger(MesosMPIMasterStarter.class.getName());

  private Config config;
  private String jobName;
  private JobMasterClient jobMasterClient;

  private MesosMPIMasterStarter() {
  }

  public static void main(String[] args) throws Exception {


    MesosMPIMasterStarter mpiMaster = new MesosMPIMasterStarter();
    //Thread.sleep(5000);
    //gets the docker home directory
    String homeDir = System.getenv("HOME");
    int workerId = Integer.parseInt(System.getenv("WORKER_ID"));
    mpiMaster.jobName = System.getenv("JOB_NAME");

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    mpiMaster.config = ConfigLoader.loadConfig(twister2Home, configDir);

    MesosWorkerLogger logger = new MesosWorkerLogger(mpiMaster.config,
        "/persistent-volume/logs", "mpiMaster");
    logger.initLogging();

    MesosWorkerController workerController = null;
    List<WorkerNetworkInfo> workerNetworkInfoList = new ArrayList<>();
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + mpiMaster.jobName + ".job");
      workerController = new MesosWorkerController(mpiMaster.config, job,
          Inet4Address.getLocalHost().getHostAddress(), 2023, workerId);
      LOG.info("Initializing with zookeeper");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerNetworkInfoList = workerController.waitForAllWorkersToJoin(
          ZKContext.maxWaitTimeForAllWorkersToJoin(mpiMaster.config));
      LOG.info("Everyone has joined");
      //container.execute(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      LOG.severe("Host unkown " + e.getMessage());
    }



    ZKJobMasterFinder finder = new ZKJobMasterFinder(mpiMaster.config);
    finder.initialize();

    String jobMasterIPandPort = finder.getJobMasterIPandPort();
    if (jobMasterIPandPort == null) {
      LOG.info("Job Master has not joined yet. Will wait and try to get the address ...");
      jobMasterIPandPort = finder.waitAndGetJobMasterIPandPort(20000);
      LOG.info("Job Master address: " + jobMasterIPandPort);
    } else {
      LOG.info("Job Master address: " + jobMasterIPandPort);
    }

    finder.close();



    //old way of finding
    //String jobMasterIP = workerNetworkInfoList.get(0).getWorkerIP().getHostAddress();

    String jobMasterPort = jobMasterIPandPort.substring(jobMasterIPandPort.lastIndexOf(":") + 1);
    String jobMasterIP = jobMasterIPandPort.substring(0, jobMasterIPandPort.lastIndexOf(":"));
    LOG.info("JobMaster IP..: " + jobMasterIP);
    LOG.info("Worker ID..: " + workerId);
    StringBuilder outputBuilder = new StringBuilder();
    int workerCount = workerController.getNumberOfWorkers();
    LOG.info("Worker Count..: " + workerCount);

    mpiMaster.startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);

    Writer writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream("/twister2/hostFile", true)));

    for (int i = 0; i < workerCount; i++) {

      writer.write(workerNetworkInfoList.get(i).getWorkerIP().getHostAddress()
          + "\n");
      LOG.info("Host IP..: " + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress());
    }

    writer.close();

    //mpi master has the id equals to 1
    //id==0 is job master
    String mpiClassNameToRun = "edu.iu.dsc.tws.rsched.schedulers.mesos.mpi.MesosMPIWorkerStarter";

    LOG.info("Before mpirun");
    String[] command = {"mpirun", "-allow-run-as-root", "-npernode",
        "1", "--mca", "btl_tcp_if_include", "eth0",
        "--hostfile", "/twister2/hostFile", "java", "-cp",
        "twister2-job/libexamples-java.jar:twister2-core/lib/*",
        mpiClassNameToRun, mpiMaster.jobName, jobMasterIP};

    LOG.info("command:" + String.join(" ", command));

    ProcessUtils.runSyncProcess(false, command, outputBuilder,
        new File("."), true);

    mpiMaster.jobMasterClient.sendWorkerCompletedMessage();
    mpiMaster.jobMasterClient.close();
    workerController.close();
    LOG.info("Job DONE");


  }

  public void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    LOG.info("JobMaster IP..: " + jobMasterIP);
    LOG.info("NETWORK INFO..: " + networkInfo.getWorkerIP().toString());
    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.startThreaded();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }


}
