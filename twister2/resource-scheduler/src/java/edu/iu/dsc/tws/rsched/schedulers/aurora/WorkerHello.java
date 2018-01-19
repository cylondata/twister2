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

import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.mpi.MPIContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static edu.iu.dsc.tws.common.config.Context.DIR_PREFIX_FOR_JOB_ARCHIVE;

public final class WorkerHello {
  public static final Logger LOG = Logger.getLogger(WorkerHello.class.getName());

  private WorkerHello() {
  }

  public static void main(String[] args) {

    String jobDescFile = System.getProperty(SchedulerContext.JOB_DESCRIPTION_FILE_CMD_VAR);
    jobDescFile = DIR_PREFIX_FOR_JOB_ARCHIVE + jobDescFile;
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    System.out.println("Hellllooo from WorkerHello class");
    System.out.println("I am working");
    System.out.println();
    System.out.println("Job description file: " + jobDescFile);
    printJob(job);

    Config config = loadConfig();
    config = overrideConfigs(job, config);

    System.out.println();
    System.out.println("Config from files: ");
    System.out.println(config.toString());

//    config = Config.newBuilder()
//        .putAll(job.getConfig().)
//        .build();
  }

  public static void printJob(JobAPI.Job job) {
    System.out.println("Job name: " + job.getJobName());
    System.out.println("Job file: " + job.getJobFormat().getJobFile());
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
   * loadConfig from config files
   * @return
   */
  public static Config loadConfig() {

    // first lets read the essential properties from java system properties
    String twister2Home = Paths.get("").toAbsolutePath().toString();
    String clusterType = System.getProperty(SchedulerContext.CLUSTER_TYPE);
    String configDir = twister2Home + "/" + DIR_PREFIX_FOR_JOB_ARCHIVE + clusterType;

    LOG.log(Level.INFO, String.format("Loading configuration with twister2_home: %s and "
        + "configuration: %s", twister2Home, configDir));
    Config config = ConfigLoader.loadConfig(twister2Home, configDir);
    return Config.newBuilder().
        putAll(config).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.TWISTER2_CONF.getKey(), configDir).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).
        build();
  }

  /**
   * configs from job object will override the ones in config from files if any
   * @return
   */
  public static Config overrideConfigs(JobAPI.Job job, Config config) {

    Config.Builder builder = Config.newBuilder().putAll(config);

    JobAPI.Config conf = job.getConfig();
    System.out.println("\nnumber of configs to override from job conf: " + conf.getKvsCount());
    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      System.out.println(kv.getKey() + ": " + kv.getValue());
      builder.put(kv.getKey(), kv.getValue());
    }

    return builder.build();
  }


}
