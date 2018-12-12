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

import java.net.Inet4Address;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerController;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class MesosMPISlaveStarter {

  public static final Logger LOG = Logger.getLogger(MesosMPISlaveStarter.class.getName());
  private static Config config;
  private static String jobName;
  private static int workerID;
  private static int resourceIndex = 0;
  private static int startingPort = 30000;

  private MesosMPISlaveStarter() { }

  public static void main(String[] args) throws Exception {

    //Thread.sleep(5000);
    workerID = Integer.parseInt(System.getenv("WORKER_ID"));
    jobName = System.getenv("JOB_NAME");
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    config = ConfigLoader.loadConfig(twister2Home, configDir);
    resourceIndex = Integer.parseInt(System.getenv("COMPUTE_RESOURCE_INDEX"));

    Map<String, Integer> additionalPorts =
        MesosWorkerUtils.generateAdditionalPorts(config, startingPort);

    MesosWorkerController workerController;
    List<JobMasterAPI.WorkerInfo> workerNetworkInfoList = new ArrayList<>();
    try {

      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");
      JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, resourceIndex);
      workerController = new MesosWorkerController(config, job,
          Inet4Address.getLocalHost().getHostAddress(), 2023, workerID, computeResource,
          additionalPorts);
      LOG.info("Initializing with zookeeper ");
      workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      workerNetworkInfoList = workerController.getAllWorkers();
      LOG.info("Everyone has joined");
      Thread.sleep(30000);
      workerController.close();

    } catch (Exception e) {
      LOG.severe("Host unknown " + e.getMessage());
    }
    Thread.sleep(3000000);

  }
}
