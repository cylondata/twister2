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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

public final class BasicKubernetesJob {
  private static final Logger LOG = Logger.getLogger(BasicKubernetesJob.class.getName());

  private BasicKubernetesJob() {
  }

  public static void main(String[] args) {

//    LoggingHelper.setupLogging(null, "logs", "client");
    LOG.info("Job submission time: " + System.currentTimeMillis());

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    LOG.fine("read config values: " + config.size() + "\n" + config);

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
    int diskMegaBytes = (int) (SchedulerContext.workerVolatileDisk(config) * 1024);
    String jobName = SchedulerContext.jobName(config);
    ResourceContainer resourceContainer = new ResourceContainer(cpus, ramMegaBytes, diskMegaBytes);

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

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
    StringBuffer logBuffer = new StringBuffer();
    logBuffer.append("Usage: Currently following actions are supported: \n");
    logBuffer.append("\tedu.iu.dsc.tws.examples.basic.BasicKubernetesJob submit\n");
    logBuffer.append("\tedu.iu.dsc.tws.examples.basic.BasicKubernetesJob terminate\n");
    LOG.severe(logBuffer.toString());
  }
}
