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
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;

import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class BasicAuroraJob {
  private BasicAuroraJob() {
  }

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    System.out.println("read config values: " + config.size());
    System.out.println(config);

    int cpus = Integer.parseInt(AuroraContext.cpusPerContainer(config));
    int ramMegaBytes = AuroraContext.ramPerContainer(config) / (1024 * 1024);
    int diskMegaBytes = AuroraContext.diskPerContainer(config) / (1024 * 1024);
    int containers = Integer.parseInt(AuroraContext.numberOfContainers(config));
    String jobName = SchedulerContext.jobName(config);
    ResourceContainer resourceContainer = new ResourceContainer(cpus, ramMegaBytes, diskMegaBytes);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    String containerClass = SchedulerContext.containerClass(config);

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName(jobName)
        .setContainerClass(containerClass)
        .setRequestResource(resourceContainer, containers)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);

    // now terminate the job
    terminateJob(config);
//    jobWriteTest(basicJob);
//    jobReadTest();
  }

  /**
   * wait some time and terminate the job
   */
  public static void terminateJob(Config config) {

    long waitTime = 100000;
    try {
      System.out.println("Waiting " + waitTime + " ms. Will terminate the job afterward .... ");
      Thread.sleep(waitTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    String jobName = "basic-aurora";
    Twister2Submitter.terminateJob(jobName, config);
  }

  /**
   * test method for BasicJob write to a file
   */
  public static void jobWriteTest(BasicJob basicJob) {
    String file = "testJobFile";
    JobUtils.writeJobFile(basicJob.serialize(), file);
  }

  /**
   * test method to read BasicJob file
   */
  public static void jobReadTest() {
    String fl = "/tmp/basic-aurora/basic-aurora3354891958097304472/twister2-core/basic-aurora.job";
    JobAPI.Job job = JobUtils.readJobFile(null, fl);
    System.out.println("job name: " + job.getJobName());
    System.out.println("job container class name: " + job.getContainer().getClassName());
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
