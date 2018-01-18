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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class WorkerHello {
  public static final Logger LOG = Logger.getLogger(WorkerHello.class.getName());

  private WorkerHello() {
  }

  public static void main(String[] args) {

    String jobDescFile = System.getProperty(SchedulerContext.JOB_DESCRIPTION_FILE_CMD_VAR);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    System.out.println("Hellllooo from WorkerHello class");
    System.out.println("I am working");
    System.out.println();
    System.out.println("Job description file: " + jobDescFile);
    printJob(job);
  }

  public static void printJob(JobAPI.Job job) {
    System.out.println("Job name: " + job.getJobName());
    System.out.println("job containers: " + job.getJobResources().getNoOfContainers());
    System.out.println("CPUs: " + job.getJobResources().getContainer().getAvailableCPU());
    System.out.println("RAM: " + job.getJobResources().getContainer().getAvailableMemory());
    System.out.println("Disk: " + job.getJobResources().getContainer().getAvailableDisk());

    JobAPI.Config conf = job.getConfig();
    System.out.println("\nnumber of key-values in job conf: " + conf.getKvsCount());
    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      System.out.println(kv.getKey() + ": " + kv.getValue());
    }
  }

  /**
   * loadConfig from config files and also from envirobnment variables
   * @param cfg the config values in this map will be put into returned Config
   * @return
   */
  public static Config loadConfig(Map<String, Object> cfg) {

    // first lets read the essential properties from java system properties
    String twister2Home = System.getProperty(SchedulerContext.TWISTER_2_HOME);
    String configDir = System.getProperty(SchedulerContext.CONFIG_DIR);
    String clusterType = System.getProperty(SchedulerContext.CLUSTER_TYPE);
    // lets get the job jar file from system properties or environment
    String jobJar = System.getProperty(SchedulerContext.USER_JOB_JAR_FILE);

    // now lets see weather these are overridden in environment variables
    Map<String, Object> environmentProperties = JobUtils.readCommandLineOpts();

    if (environmentProperties.containsKey(SchedulerContext.TWISTER_2_HOME)) {
      twister2Home = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CONFIG_DIR)) {
      configDir = (String) environmentProperties.get(SchedulerContext.CONFIG_DIR);
    }

    if (environmentProperties.containsKey(SchedulerContext.CLUSTER_TYPE)) {
      clusterType = (String) environmentProperties.get(SchedulerContext.CLUSTER_TYPE);
    }

    if (environmentProperties.containsKey(SchedulerContext.USER_JOB_JAR_FILE)) {
      jobJar = (String) environmentProperties.get(SchedulerContext.USER_JOB_JAR_FILE);
    }

    if (configDir == null) {
      configDir = twister2Home + "/conf";
    }

    LOG.log(Level.INFO, String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s and cluster: %s", twister2Home, configDir, clusterType));
    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().
        putAll(config).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(MPIContext.USER_JOB_JAR_FILE, jobJar).
        putAll(environmentProperties).
        putAll(cfg).
        build();
  }




}
