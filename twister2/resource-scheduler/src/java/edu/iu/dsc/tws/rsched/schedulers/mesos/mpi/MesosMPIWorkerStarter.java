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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.discovery.IWorkerDiscoverer;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
//import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosVolatileVolume;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerController;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerLogger;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIWorker;
import edu.iu.dsc.tws.rsched.spi.container.IPersistentVolume;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import mpi.MPI;
import mpi.MPIException;

public final class MesosMPIWorkerStarter {

  public static final Logger LOG = Logger.getLogger(MesosMPIWorkerStarter.class.getName());
  private static Config config;
  private static String jobName;
  private static JobMasterClient jobMasterClient;
  private static int workerID;
  private static int numberOfWorkers;

  private MesosMPIWorkerStarter() { }
  public static void main(String[] args) throws Exception {

    Thread.sleep(5000);
    //gets the docker home directory
    // String homeDir = System.getenv("HOME");
    try {
      MPI.Init(args);
      workerID = MPI.COMM_WORLD.getRank();
      numberOfWorkers = MPI.COMM_WORLD.getSize();
      System.out.println("worker iidddddd.........:" + workerID + " " + numberOfWorkers);
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Could not get rank or size from mpi.COMM_WORLD", e);
      throw new RuntimeException(e);
    }
    ///workerID++;
    //int workerId = Integer.parseInt(System.getenv("WORKER_ID"));
    jobName = args[0];
    System.out.println("job name......................:::" + jobName);
    int id = workerID;

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    config = ConfigLoader.loadConfig(twister2Home, configDir);

    MesosWorkerLogger logger = new MesosWorkerLogger(config,
        "/persistent-volume/logs", "worker" + workerID);
    logger.initLogging();

    MesosWorkerController workerController = null;
    List<WorkerNetworkInfo> workerNetworkInfoList = new ArrayList<>();
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");
      workerController = new MesosWorkerController(config, job,
          Inet4Address.getLocalHost().getHostAddress(), 2022, id);
      LOG.info("Initializing with zookeeper.." + Inet4Address.getLocalHost().getHostAddress());
      LOG.info("worker id is....:" + workerID);
      //workerController.initializeWithZooKeeper();
      LOG.info("Waiting for all workers to join");
      //workerNetworkInfoList = workerController.waitForAllWorkersToJoin(
      //    ZKContext.maxWaitTimeForAllWorkersToJoin(config));
      LOG.info("Everyone has joined");
      //container.init(worker.config, id, null, workerController, null);

    } catch (Exception e) {
      e.printStackTrace();
    }

    //String jobMasterIP = workerNetworkInfoList.get(0).getWorkerIP().getHostAddress();
    String jobMasterIP = args[1];
    LOG.info("JobMasterIP" + jobMasterIP);
    System.out.println("Worker id " + id);
    //int workerCount = workerController.getNumberOfWorkers();
    //System.out.println("worker count " + workerCount);
    startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);
    System.out.println("\nworker controller\nworker id..:"
        + workerController.getWorkerNetworkInfo().getWorkerID()
        + "ip address..:" + workerController.getWorkerNetworkInfo().getWorkerIP().toString());
    startWorker(workerController, null);


    try {
      MPI.Finalize();
    } catch (MPIException ignore) { }

    closeWorker();
  }

  public static void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    LOG.info("JobMasterIP: " + jobMasterIP);

    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.init();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }

  public static void startWorker(IWorkerDiscoverer workerController,
                                 IPersistentVolume pv) {
    String workerClass = SchedulerContext.containerClass(config);
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the worker class %s", workerClass));
      throw new RuntimeException(e);
    }

    MesosVolatileVolume volatileVolume = null;
    if (SchedulerContext.volatileDiskRequested(config)) {
      volatileVolume =
          new MesosVolatileVolume(SchedulerContext.jobName(config), workerID);
    }

    ResourcePlan resourcePlan = MPIWorker.createResourcePlan(config);

    worker.init(config, workerID, resourcePlan, workerController, pv, volatileVolume);
  }

  /**
   * last method to call to close the worker
   */
  public static void closeWorker() {

    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    jobMasterClient.sendWorkerCompletedMessage();
    jobMasterClient.close();
  }

}
