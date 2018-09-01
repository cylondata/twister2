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
package edu.iu.dsc.tws.rsched.interfaces;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.RequestedResources;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * Launches job. The purpose of the launcher is to bring up the required processes.
 * After it brings up the required processes, the controller is used for managing the job.
 */
public interface ILauncher extends AutoCloseable {
  /**
   * Initialize with the configuration
   *
   * @param config the configuration
   */
  void initialize(Config config);

  /**
   * Cleanup any resources
   */
  void close();

  /**
   * terminate the submitted job
   */
  boolean terminateJob(String jobName);

  /**
   * Launch the processes according to the resource plan. An implementation fo this class will
   *
   * @param resourceRequest requested resources
   * @return true if the request is granted
   */
  boolean launch(RequestedResources resourceRequest, JobAPI.Job job);
}
