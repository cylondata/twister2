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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosVolatileVolume;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerController;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerLogger;
import edu.iu.dsc.tws.rsched.schedulers.mesos.MesosWorkerUtils;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIWorker;
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
  public static void main(String[] args) {

    try {
      MPI.Init(args);
      workerID = MPI.COMM_WORLD.getRank();
      numberOfWorkers = MPI.COMM_WORLD.getSize();
      System.out.println("Worker ranking..:" + workerID
          + " Number of workers..:" + numberOfWorkers);

    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Could not get rank or size from mpi.COMM_WORLD", e);
      throw new RuntimeException(e);
    }

    jobName = args[0];

    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String configDir = "twister2-job/mesos/";
    config = ConfigLoader.loadConfig(twister2Home, configDir);

    MesosWorkerLogger logger = new MesosWorkerLogger(config,
        "/persistent-volume/logs", "worker" + workerID);
    logger.initLogging();

    MesosWorkerController workerController = null;
    //List<WorkerNetworkInfo> workerNetworkInfoList = new ArrayList<>();
    try {
      JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/"
          + jobName + ".job");

      // add any configuration from job file to the config object
      // if there are the same config parameters in both,
      // job file configurations will override
      config = JobUtils.overrideConfigs(job, config);
      config = JobUtils.updateConfigs(job, config);

      LOG.info("in worker starter...... job worker count:" + job.getNumberOfWorkers());
      workerController = new MesosWorkerController(config, job,
          Inet4Address.getLocalHost().getHostAddress(), 2023, workerID);
      workerController.initializeWithZooKeeper();
    } catch (Exception e) {
      LOG.severe("Error " + e.getMessage());
    }

    //can not access docker env variable so it was passed as a parameter
    String jobMasterIP = args[1];
    LOG.info("JobMaster IP..: " + jobMasterIP);
    LOG.info("Worker ID..: " + workerID);
    startJobMasterClient(workerController.getWorkerNetworkInfo(), jobMasterIP);

    LOG.info("\nWorker Controller\nWorker ID..: "
        + workerController.getWorkerNetworkInfo().getWorkerID()
        + "\nIP address..: " + workerController.getWorkerNetworkInfo().getWorkerIP().toString());

    startWorker(workerController, null);
    try {
      MPI.Finalize();
    } catch (MPIException ignore) {
      LOG.info("MPI Finalize Exception" + ignore.getMessage());
    }

    closeWorker();
    //workerController.close();
  }

  public static void startJobMasterClient(WorkerNetworkInfo networkInfo, String jobMasterIP) {

    LOG.info("JobMaster IP..: " + jobMasterIP);
    LOG.info("NETWORK INFO..: " + networkInfo.getWorkerIP().toString());
    jobMasterClient = new JobMasterClient(config, networkInfo, jobMasterIP);
    jobMasterClient.startThreaded();
    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();
  }

  public static void startWorker(IWorkerController workerController,
                                 IPersistentVolume pv) {


    JobAPI.Job job = JobUtils.readJobFile(null, "twister2-job/" + jobName + ".job");
    String workerClass = job.getWorkerClassName();
    LOG.info("Worker class---->>>" + workerClass);
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("Loaded worker class..: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("Failed to load the worker class %s", workerClass));
      throw new RuntimeException(e);
    }

    MesosVolatileVolume volatileVolume = null;
    if (SchedulerContext.volatileDiskRequested(config)) {
      volatileVolume =
          new MesosVolatileVolume(SchedulerContext.jobName(config), workerID);
    }
    // lets create the resource plan
    Map<Integer, String> processNames = MPIWorker.createResourcePlan(config);
    // now create the resource plan
    //AllocatedResources resourcePlan = MPIWorker.addContainers(config, processNames);
    AllocatedResources resourcePlan = MesosWorkerUtils.createAllocatedResources("mesos",
        workerID, job);
    //resourcePlan = new AllocatedResources(SchedulerContext.clusterType(config), workerID);
    worker.execute(config, workerID, resourcePlan, workerController, pv, volatileVolume);
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
