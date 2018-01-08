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
import edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraClientContext;
import edu.iu.dsc.tws.rsched.schedulers.aurora.WorkerHello;
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

    int cpus = Integer.parseInt(AuroraClientContext.cpusPerContainer(config));
    int ramMegaBytes = AuroraClientContext.ramPerContainer(config) / (1024 * 1024);
    int diskMegaBytes = AuroraClientContext.diskPerContainer(config) / (1024 * 1024);
    int containers = Integer.parseInt(AuroraClientContext.cpusPerContainer(config));
    ResourceContainer resourceContainer = new ResourceContainer(cpus, ramMegaBytes, diskMegaBytes);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putConfig(config);

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName("basic-aurora")
        .setContainerClass(WorkerHello.class.getName())
        .setRequestResource(resourceContainer, containers)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
//    jobWriteTest(basicJob);
//    jobReadTest();
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
    String file = "testJobFile";
    JobAPI.Job job = JobUtils.readJobFile(null, file);
    System.out.println("job name: " + job.getJobName());
    System.out.println("job container class name: " + job.getContainer().getClassName());
    System.out.println("job containers: " + job.getJobResources().getNoOfContainers());
    System.out.println("CPUs: " + job.getJobResources().getContainer().getAvailableCPU());
    System.out.println("RAM: " + job.getJobResources().getContainer().getAvailableMemory());
    System.out.println("Disk: " + job.getJobResources().getContainer().getAvailableDisk());
    JobAPI.Config conf = job.getConfig();
    System.out.println("number of key-values in conf: " + conf.getKvsCount());

    for (JobAPI.Config.KeyValue kv : conf.getKvsList()) {
      System.out.println(kv.getKey() + ": " + kv.getValue());
    }

  }

}
