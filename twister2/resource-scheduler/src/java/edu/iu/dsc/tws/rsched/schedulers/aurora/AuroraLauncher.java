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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKUtil;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.interfaces.ILauncher;

/**
 * submit a job to Aurora Scheduler using AuroraClientController
 */

public class AuroraLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;

  @Override
  public void initialize(Config conf) {
    this.config = conf;
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @param resourceRequest requested resources
   * @return true if the request is granted
   */
  @Override
  public boolean launch(RequestedResources resourceRequest, JobAPI.Job job) {

    String jobName = job.getJobName();

    // first check whether there is an active job running with same name on ZooKeeper
    if (ZKUtil.isThereAnActiveJob(jobName, config)) {
      throw new RuntimeException("There is an active job in ZooKeeper with same name."
          + "\nFirst try to kill that job. Run terminate job command."
          + "\nThis job is not submitted to Aurora Server");
    }

    //construct the controller to submit the job to Aurora Scheduler
    String cluster = AuroraContext.auroraClusterName(config);
    String role = AuroraContext.role(config);
    String env = AuroraContext.environment(config);
    AuroraClientController controller =
        new AuroraClientController(cluster, role, env, jobName, true);

    // get aurora file name to execute when submitting the job
    String auroraFilename = AuroraContext.auroraScript(config);

    // get environment variables from config
    Map<AuroraField, String> bindings = constructEnvVariables(config);

    // convert RequestedResources to environment variables, override previous values from config
    WorkerComputeResource computeResource = resourceRequest.getWorkerComputeResource();
    bindings.put(AuroraField.JOB_NAME, jobName);
    bindings.put(AuroraField.AURORA_WORKER_CLASS, AuroraContext.auroraWorkerClass(config));
    bindings.put(AuroraField.CPUS_PER_WORKER, computeResource.getNoOfCpus() + "");
    bindings.put(AuroraField.RAM_PER_WORKER, computeResource.getMemoryInBytes() + "");
    bindings.put(AuroraField.DISK_PER_WORKER, computeResource.getDiskInBytes() + "");
    bindings.put(AuroraField.NUMBER_OF_WORKERS, resourceRequest.getNumberOfWorkers() + "");

    logEnvVariables(bindings);

    return controller.createJob(bindings, auroraFilename);
  }

  /**
   * Close up any resources
   */
  @Override
  public void close() {
  }

  /**
   * Terminate the Aurora Job
   */
  @Override
  public boolean terminateJob(String jobName) {

    //construct the controller to submit the job to Aurora Scheduler
    String cluster = AuroraContext.auroraClusterName(config);
    String role = AuroraContext.role(config);
    String env = AuroraContext.environment(config);
    AuroraClientController controller =
        new AuroraClientController(cluster, role, env, jobName, true);

    boolean killedAuroraJob = controller.killJob();
    if (killedAuroraJob) {
      LOG.log(Level.INFO, "Aurora job kill command succeeded.");
    } else {
      LOG.log(Level.SEVERE, "Aurora job kill command failed.");
    }

    // first clear ZooKeeper data
    boolean deletedZKNodes = ZKUtil.terminateJob(jobName, config);

    return killedAuroraJob && deletedZKNodes;
  }

  /**
   * put relevant config parameters to a HashMap to be used as environment variables
   * when submitting jobs
   * @param config
   * @return
   */
  public static Map<AuroraField, String> constructEnvVariables(Config config) {

    String jobName = SchedulerContext.jobName(config);
    String jobDescriptionFile = SchedulerContext.createJobDescriptionFileName(jobName);

    HashMap<AuroraField, String> envs = new HashMap<AuroraField, String>();
    envs.put(AuroraField.CORE_PACKAGE_FILENAME, SchedulerContext.corePackageFileName(config));
    envs.put(AuroraField.JOB_PACKAGE_FILENAME, SchedulerContext.jobPackageFileName(config));
    envs.put(AuroraField.AURORA_CLUSTER_NAME, AuroraContext.auroraClusterName(config));
    envs.put(AuroraField.ENVIRONMENT, AuroraContext.environment(config));
    envs.put(AuroraField.ROLE, AuroraContext.role(config));
    envs.put(AuroraField.JOB_NAME, jobName);
    envs.put(AuroraField.CPUS_PER_WORKER, SchedulerContext.workerCPU(config) + "");
    envs.put(AuroraField.RAM_PER_WORKER, SchedulerContext.workerRAM(config) * 1048576 + "");
    envs.put(AuroraField.DISK_PER_WORKER,
        AuroraContext.workerVolatileDisk(config) * 1073741824 + "");
    envs.put(AuroraField.NUMBER_OF_WORKERS, SchedulerContext.workerInstances(config) + "");
    envs.put(AuroraField.TWISTER2_PACKAGES_PATH, SchedulerContext.packagesPath(config));
    envs.put(AuroraField.JOB_DESCRIPTION_FILE, jobDescriptionFile);
    envs.put(AuroraField.USER_JOB_JAR_FILE, SchedulerContext.userJobJarFile(config));
    envs.put(AuroraField.CLUSTER_TYPE, SchedulerContext.clusterType(config));
    return envs;
  }

  /**
   * print all environment variables for debugging purposes
   * @param envs
   */
  public static void logEnvVariables(Map<AuroraField, String> envs) {

    Set<AuroraField> keys = envs.keySet();
    StringBuffer sb = new StringBuffer("All environment variables:");
    for (AuroraField key: keys) {
      sb.append("\n" + key + ": " + envs.get(key));
    }
    LOG.log(Level.INFO, sb.toString());
  }
}
