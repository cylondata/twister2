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
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.spi.resource.RequestedResources;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.scheduler.ILauncher;

/**
 * submit a job to Aurora Scheduler using AuroraClientController
 */

public class AuroraLauncher implements ILauncher{
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());

  private Config config;
  AuroraClientController controller;

  @Override
  public void initialize(Config config){
    this.config = config;

    //construct the controller to submit the job to Aurora Scheduler
    String cluster = AuroraClientContext.cluster(config);
    String role = AuroraClientContext.role(config);
    String env = AuroraClientContext.environment(config);
    String jobName = AuroraClientContext.auroraJobName(config);
    controller = new AuroraClientController(cluster, role, env, jobName, true);
  }

  /**
   * Launch the processes according to the resource plan.
   *
   * @param resourceRequest requested resources
   * @return true if the request is granted
   */
  @Override
  public boolean launch(RequestedResources resourceRequest, JobAPI.Job job){

    // get aurora file name to execute when submitting the job
    String auroraFilename = AuroraClientContext.auroraScript(config);

    // get environment variables from config
    Map<AuroraField, String> bindings = AuroraJobSubmitter.constructEnvVariables(config);

    // convert RequestedResources to environment variables
    ResourceContainer container = resourceRequest.getContainer();
    bindings.put(AuroraField.CPUS_PER_CONTAINER, container.getNoOfCpus()+"");
    bindings.put(AuroraField.RAM_PER_CONTAINER, container.getMemoryInBytes()+"");
    bindings.put(AuroraField.DISK_PER_CONTAINER, container.getDiskInBytes()+"");
    bindings.put(AuroraField.NUMBER_OF_CONTAINERS, resourceRequest.getNoOfContainers()+"");

    AuroraJobSubmitter.printEnvs(bindings);

    return controller.createJob(bindings, auroraFilename);
  }

  /**
   * Cleanup any resources
   */
  @Override
  public void close(){

  }
}
