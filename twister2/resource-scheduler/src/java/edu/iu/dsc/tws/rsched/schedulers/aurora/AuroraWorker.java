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
package edu.iu.dsc.tws.rsched.schedulers.aurora;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.WorkerInfo;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKController;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;

public final class AuroraWorker {
  public static final Logger LOG = Logger.getLogger(AuroraWorker.class.getName());

  private InetAddress workerAddress;
  private int workerPort;
  private String mesosTaskID;
  private Config config;
  private JobAPI.Job job;
  private ZKController zkController;

  private AuroraWorker() {
  }

  /**
   * create a AuroraWorker object by getting values from system property
   * @return
   */
  public static AuroraWorker createAuroraWorker() {
    AuroraWorker worker = new AuroraWorker();
    String hostname =  System.getProperty("hostname");
    String portStr =  System.getProperty("tcpPort");
    worker.mesosTaskID = System.getProperty("taskID");
    try {
      worker.workerAddress = InetAddress.getByName(hostname);
      worker.workerPort = Integer.parseInt(portStr);
      LOG.log(Level.INFO, "worker IP: " + hostname + " workerPort: " + portStr);
      LOG.log(Level.INFO, "worker mesosTaskID: " + worker.mesosTaskID);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "worker ip address is not valid: " + hostname, e);
      throw new RuntimeException(e);
    }

    // read job description file
    worker.readJobDescFile();
    printJob(worker.job);

    // load config files
    worker.loadConfig();
//    System.out.println("Config from files: ");
//    System.out.println(worker.config.toString());

    // override config files with values from job config if any
    worker.overrideConfigsFromJob();

    // get unique workerID and let other workers know about this worker in the job
    worker.initializeWithZooKeeper();

    return worker;
  }

  /**
   * read job description file and construct job object
   */
  private void readJobDescFile() {
    String jobDescFile = System.getProperty(SchedulerContext.JOB_DESCRIPTION_FILE_CMD_VAR);
    jobDescFile = DIR_PREFIX_FOR_JOB_ARCHIVE + jobDescFile;
    job = JobUtils.readJobFile(null, jobDescFile);

    // printing for testing
    LOG.log(Level.INFO, "Job description file is read: " + jobDescFile);
  }

  /**
   * loadConfig from config files
   * @return
   */
  public void loadConfig() {

    // first lets read the essential properties from java system properties
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String clusterType = System.getProperty(SchedulerContext.CLUSTER_TYPE);
    String configDir = twister2Home + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + clusterType;

    LOG.log(Level.INFO, String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s", twister2Home, configDir));
    Config conf = ConfigLoader.loadConfig(twister2Home, configDir);
    config = Config.newBuilder().
        putAll(conf).
        put(Context.TWISTER2_HOME.getKey(), twister2Home).
        put(Context.TWISTER2_CONF.getKey(), configDir).
        put(Context.TWISTER2_CLUSTER_TYPE, clusterType).
        build();

    LOG.log(Level.INFO, "Config files are read from directory: " + configDir);
  }

  /**
   * configs from job object will override the ones in config from files if any
   * @return
   */
  public void overrideConfigsFromJob() {

    Config.Builder builder = Config.newBuilder().putAll(config);

    JobAPI.Config conf = job.getConfig();
    LOG.log(Level.INFO, "Number of configs to override from job conf: " + conf.getKvsCount());

    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      builder.put(kv.getKey(), kv.getValue());
      LOG.log(Level.INFO, "Overriden conf key-value pair: " + kv.getKey() + ": " + kv.getValue());
    }

    config = builder.build();
  }

  public void initializeWithZooKeeper() {

    long startTime = System.currentTimeMillis();
    String workerHostPort = workerAddress.getHostAddress() + ":" + workerPort;
    zkController = new ZKController(config, job.getJobName(), workerHostPort);
    zkController.initialize();
    long duration = System.currentTimeMillis() - startTime;
    System.out.println("Initialization for the worker: " + zkController.getWorkerInfo()
        + " took: " + duration + "ms");
  }

  /**
   * needs to close down when finished computation
   */
  public void waitAndGetAllWorkers() {
    int numberOfWorkers = job.getJobResources().getNoOfContainers();
    System.out.println("Waiting for " + numberOfWorkers + " workers to join .........");

    // the amount of time to wait for all workers to join a job
    int timeLimit =  ZKContext.maxWaitTimeForAllWorkersToJoin(config);
    long startTime = System.currentTimeMillis();
    List<WorkerInfo> workers = zkController.waitForAllWorkersToJoin(numberOfWorkers, timeLimit);
    long duration = System.currentTimeMillis() - startTime;

    if (workers == null) {
      LOG.log(Level.SEVERE, "Could not get full worker list. timeout limit has been reached !!!!"
          + "Waited " + timeLimit + " ms.");
    } else {
      LOG.log(Level.INFO, "Waited " + duration + " ms for all workers to join.");

      System.out.println("list of current workers in the job: ");
      zkController.printWorkers(workers);

      System.out.println();
      System.out.println("list of all joined workers to the job: ");
      zkController.printWorkers(zkController.getAllJoinedWorkers());
    }
  }

  /**
   * needs to close down when finished computation
   */
  public void close() {
    zkController.close();
  }

  public static void main(String[] args) {

    // create the worker
    AuroraWorker worker = createAuroraWorker();

    // get the number of workers from some where
    // wait for all of them
    // print their list and exit
    worker.waitAndGetAllWorkers();

    String containerClass = SchedulerContext.containerClass(worker.config);
    IContainer container;
    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      container = (IContainer) object;
      LOG.info("loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
      throw new RuntimeException(e);
    }

    container.init(worker.config, worker.zkController.getWorkerInfo().getWorkerID(), null);

    // close the things, let others know that it is done
    worker.close();
  }

  /**
   * a test method to print a job
   * @param job
   */
  public static void printJob(JobAPI.Job job) {
    System.out.println("Job name: " + job.getJobName());
    System.out.println("Job file: " + job.getJobFormat().getJobFile());
    System.out.println("job containers: " + job.getJobResources().getNoOfContainers());
    System.out.println("CPUs: " + job.getJobResources().getContainer().getAvailableCPU());
    System.out.println("RAM: " + job.getJobResources().getContainer().getAvailableMemory());
    System.out.println("Disk: " + job.getJobResources().getContainer().getAvailableDisk());

    JobAPI.Config conf = job.getConfig();
    System.out.println("number of key-values in job conf: " + conf.getKvsCount());
    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      System.out.println(kv.getKey() + ": " + kv.getValue());
    }
  }
}
