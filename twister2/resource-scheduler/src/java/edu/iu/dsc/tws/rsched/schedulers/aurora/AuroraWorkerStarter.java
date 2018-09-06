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
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKWorkerController;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static edu.iu.dsc.tws.common.config.Context.JOB_ARCHIVE_DIRECTORY;

public final class AuroraWorkerStarter {
  public static final Logger LOG = Logger.getLogger(AuroraWorkerStarter.class.getName());

  private InetAddress workerAddress;
  private int workerPort;
  private String mesosTaskID;
  private Config config;
  private JobAPI.Job job;
  private ZKWorkerController zkWorkerController;

  private AuroraWorkerStarter() {
  }

  public static void main(String[] args) {

    // create the worker
    AuroraWorkerStarter workerStarter = createAuroraWorker();

    // get the number of workers from some where
    // wait for all of them
    // print their list and exit
    workerStarter.waitAndGetAllWorkers();

    String workerClass = SchedulerContext.workerClass(workerStarter.config);
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
          workerClass), e);
      throw new RuntimeException(e);
    }

    // TODO: need toprovide all parameters
    worker.execute(workerStarter.config,
        workerStarter.zkWorkerController.getWorkerNetworkInfo().getWorkerID(),
        null, null, null, null);

    // close the things, let others know that it is done
    workerStarter.close();
  }

  /**
   * create a AuroraWorkerStarter object by getting values from system property
   * @return
   */
  public static AuroraWorkerStarter createAuroraWorker() {
    AuroraWorkerStarter workerStarter = new AuroraWorkerStarter();
    String hostname =  System.getProperty("hostname");
    String portStr =  System.getProperty("tcpPort");
    workerStarter.mesosTaskID = System.getProperty("taskID");
    try {
      workerStarter.workerAddress = InetAddress.getByName(hostname);
      workerStarter.workerPort = Integer.parseInt(portStr);
      LOG.log(Level.INFO, "worker IP: " + hostname + " workerPort: " + portStr);
      LOG.log(Level.INFO, "worker mesosTaskID: " + workerStarter.mesosTaskID);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "worker ip address is not valid: " + hostname, e);
      throw new RuntimeException(e);
    }

    // read job description file
    workerStarter.readJobDescFile();
    logJobInfo(workerStarter.job);

    // load config files
    workerStarter.loadConfig();
    LOG.fine("Config from files: \n" + workerStarter.config.toString());

    // override config files with values from job config if any
    workerStarter.overrideConfigsFromJob();

    // get unique workerID and let other workers know about this worker in the job
    workerStarter.initializeWithZooKeeper();

    return workerStarter;
  }

  /**
   * read job description file and construct job object
   */
  private void readJobDescFile() {
    String jobDescFile = System.getProperty(SchedulerContext.JOB_DESCRIPTION_FILE_CMD_VAR);
    jobDescFile = JOB_ARCHIVE_DIRECTORY + "/" + jobDescFile;
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
    String configDir = twister2Home + "/" + JOB_ARCHIVE_DIRECTORY + "/" + clusterType;

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
    int numberOfWorkers = job.getNumberOfWorkers();

    // TODO: need to put at least nodeIP to this NodeInfo object
    NodeInfo nodeInfo = new NodeInfo(null, null, null);
    zkWorkerController =
        new ZKWorkerController(config, job.getJobName(), workerHostPort, numberOfWorkers, nodeInfo);
    zkWorkerController.initialize();
    long duration = System.currentTimeMillis() - startTime;
    LOG.info("Initialization for the worker: " + zkWorkerController.getWorkerNetworkInfo()
        + " took: " + duration + "ms");
  }

  /**
   * needs to close down when finished computation
   */
  public void waitAndGetAllWorkers() {
    int numberOfWorkers = job.getNumberOfWorkers();
    LOG.info("Waiting for " + numberOfWorkers + " workers to join .........");

    // the amount of time to wait for all workers to join a job
    int timeLimit =  ZKContext.maxWaitTimeForAllWorkersToJoin(config);
    long startTime = System.currentTimeMillis();
    List<WorkerNetworkInfo> workerList = zkWorkerController.waitForAllWorkersToJoin(timeLimit);
    long duration = System.currentTimeMillis() - startTime;

    if (workerList == null) {
      LOG.log(Level.SEVERE, "Could not get full worker list. timeout limit has been reached !!!!"
          + "Waited " + timeLimit + " ms.");
    } else {
      LOG.log(Level.INFO, "Waited " + duration + " ms for all workers to join.");

      LOG.info("list of all joined workers in the job: "
          + WorkerNetworkInfo.workerListAsString(workerList));
    }
  }

  /**
   * needs to close down when finished computation
   */
  public void close() {
    zkWorkerController.close();
  }

  /**
   * a test method to print a job
   * @param job
   */
  public static void logJobInfo(JobAPI.Job job) {
    StringBuffer sb = new StringBuffer("Job Details:");
    sb.append("\nJob name: " + job.getJobName());
    sb.append("\nJob file: " + job.getJobFormat().getJobFile());
    sb.append("\nnumber of workers: " + job.getNumberOfWorkers());
    sb.append("\nCPUs: "
        + job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getCpu());
    sb.append("\nRAM: "
        + job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getRam());
    sb.append("\nDisk: "
        + job.getJobResources().getResourcesList().get(0).getWorkerComputeResource().getDisk());

    JobAPI.Config conf = job.getConfig();
    sb.append("\nnumber of key-values in job conf: " + conf.getKvsCount());
    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      sb.append("\n" + kv.getKey() + ": " + kv.getValue());
    }

    LOG.info(sb.toString());
  }
}
