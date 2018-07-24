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
    Thread.sleep(3000);
    //gets the docker home directory
    String homeDir = System.getenv("HOME");
    int workerId = Integer.parseInt(System.getenv("WORKER_ID"));
    mpiMaster.jobName = System.getenv("JOB_NAME");
    int id = workerId;

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
          Inet4Address.getLocalHost().getHostAddress(), 2022, id);
      LOG.info("Initializing with zookeeper");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerNetworkInfoList = workerController.waitForAllWorkersToJoin(
          ZKContext.maxWaitTimeForAllWorkersToJoin(mpiMaster.config));
      LOG.info("Everyone has joined");
      //container.init(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      e.printStackTrace();
    }


    String jobMasterIP = workerNetworkInfoList.get(0).getWorkerIP().getHostAddress();
    LOG.info("JobMasterIP" + jobMasterIP);
    System.out.println("Worker id " + id);
    StringBuilder outputBuilder = new StringBuilder();
    int workerCount = workerController.getNumberOfWorkers();
    System.out.println("worker count " + workerCount);

    mpiMaster.startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);

    Writer writer = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream("/twister2/hostFile", true)));

    System.out.println("worker count is...:" + workerCount);
    for (int i = 1; i < workerCount; i++) {

      writer.write(workerNetworkInfoList.get(i).getWorkerIP().getHostAddress() + "\n");

      System.out.println("host ip: "
          + workerNetworkInfoList.get(i).getWorkerIP().getHostAddress());
    }

    writer.close();

    //mpi master has the id equals to 1
    //id==0 is job master
    String mpiClassNameToRun = "edu.iu.dsc.tws.rsched.schedulers.mesos.mpi.MesosMPIWorkerStarter";

    System.out.println("Before mpirun");
    String[] command = {"mpirun", "-x", "LD_PRELOAD=libmpi.so", "-allow-run-as-root", "-np",
        (workerController.getNumberOfWorkers() - 1) + "",
        "--hostfile", "/twister2/hostFile", "java", "-cp",
        "twister2-job/libexamples-java.jar:twister2-core/lib/*",
        mpiClassNameToRun, mpiMaster.jobName, jobMasterIP, ">mpioutfile"};

    System.out.println("command:" + String.join(" ", command));
    // Thread.sleep(5000);

    ProcessUtils.runSyncProcess(false, command, outputBuilder,
        new File("."), true);
    workerController.close();
    System.out.println("Finished");

    mpiMaster.jobMasterClient.sendWorkerCompletedMessage();
  }

  public void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    LOG.info("JobMasterIP: " + jobMasterIP);
    LOG.info("NETWORK INFO    " + networkInfo.getWorkerIP().toString());
    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.init();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }

}
