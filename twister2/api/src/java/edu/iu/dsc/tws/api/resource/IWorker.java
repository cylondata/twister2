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
package edu.iu.dsc.tws.api.resource;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This is the main point of entry from resource scheduler layer to higher layers of Twister2.
 * Resource scheduler starts an instance of this class for each worker.
 */
public interface IWorker {
  /**
   * Execute with the resources configured
   * @param config configuration
   * @param job the worker id
   * @param workerController the worker controller
   * @param persistentVolume information about persistent file system
   * @param volatileVolume information about volatile file system
   */
  void execute(Config config,
               JobAPI.Job job,
               IWorkerController workerController,
               IPersistentVolume persistentVolume,
               IVolatileVolume volatileVolume);

}
