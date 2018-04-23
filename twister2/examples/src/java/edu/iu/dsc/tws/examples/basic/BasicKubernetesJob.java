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
package edu.iu.dsc.tws.examples.basic;

import java.util.HashMap;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

public final class BasicKubernetesJob {
  private BasicKubernetesJob() {
  }

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    System.out.println("read config values: " + config.size());
    System.out.println(config);

    if (args.length == 0) {
      printUsage();
    } else if (args[0].equals("submit")) {
      submitJob(config);
    } else if (args[0].equals("terminate")) {
      terminateJob(config);
    } else {
      printUsage();
    }

  }

  /**
   * submit the job
   */
  public static void submitJob(Config config) {

    double cpus = SchedulerContext.workerCPU(config);
    int ramMegaBytes = SchedulerContext.workerRAM(config);
    int workers = SchedulerContext.workerInstances(config);
    int diskMegaBytes = AuroraContext.workerDisk(config);
    String jobName = SchedulerContext.jobName(config);
    ResourceContainer resourceContainer = new ResourceContainer(cpus, ramMegaBytes, diskMegaBytes);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    String containerClass = SchedulerContext.containerClass(config);

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName(jobName)
        .setContainerClass(containerClass)
        .setRequestResource(resourceContainer, workers)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }

  /**
   * terminate the job
   */
  public static void terminateJob(Config config) {

    String jobName = SchedulerContext.jobName(config);
    Twister2Submitter.terminateJob(jobName, config);
  }

  /**
   * print usage
   */
  public static void printUsage() {
    System.out.println("Usage: Currently following actions are supported: ");
    System.out.println("\tedu.iu.dsc.tws.examples.basic.BasicKubernetesJob submit");
    System.out.println("\tedu.iu.dsc.tws.examples.basic.BasicKubernetesJob terminate");
  }
}
