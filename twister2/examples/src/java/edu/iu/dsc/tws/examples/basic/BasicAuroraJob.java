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
import edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraClientContext;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.schedulers.aurora.WorkerHello;

public final class BasicAuroraJob {
  private BasicAuroraJob() {
  }

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    System.out.println("read config values: ");
    System.out.println(config);

    int cpus = Integer.parseInt(AuroraClientContext.cpusPerContainer(config));
    int ramMegaBytes = AuroraClientContext.ramPerContainer(config)/(1024*1024);
    int diskMegaBytes = AuroraClientContext.diskPerContainer(config)/(1024*1024);
    int containers = Integer.parseInt(AuroraClientContext.cpusPerContainer(config));
    ResourceContainer resourceContainer = new ResourceContainer(cpus, ramMegaBytes, diskMegaBytes);

    // build the job
    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("basic-aurora");
    jobBuilder.setContainerClass(WorkerHello.class.getName());
    jobBuilder.setRequestResource(resourceContainer, containers);
    BasicJob basicJob = jobBuilder.build();

    // now submit the job
    Twister2Submitter.submitAuroraJob(basicJob, config);
  }
}
