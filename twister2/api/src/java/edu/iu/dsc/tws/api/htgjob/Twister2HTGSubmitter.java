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
package edu.iu.dsc.tws.api.htgjob;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class Twister2HTGSubmitter {
  private static final Logger LOG = Logger.getLogger(Twister2HTGSubmitter.class.getName());

  private Twister2HTGSubmitter() {
  }

  /**
   * Submit a Twister2 job
   *
   * @param twister2HTGJob job
   */
  public static void submitJob(Twister2HTGJob twister2HTGJob, Config config) {

    // save the job to transfer to workers
    JobAPI.Job job = twister2HTGJob.serialize();

    // update the config object with the values from job
    Config updatedConfig = JobUtils.updateConfigs(job, config);

    // launch the luancher
    ResourceAllocator resourceAllocator = new ResourceAllocator();
    resourceAllocator.submitJob(job, updatedConfig);
  }

  /**
   * terminate a Twister2 job
   */
  @SuppressWarnings("ParameterAssignment")
  public static void terminateJob(String jobName, Config config) {

    // launch the launcher
    ResourceAllocator resourceAllocator = new ResourceAllocator();
    resourceAllocator.terminateJob(jobName, config);
  }
}
