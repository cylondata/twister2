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
package edu.iu.dsc.tws.common.worker;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;

/**
 * This is the main point of entry for a Twister2 job. Every job should implement this interface.
 * When a job is submitted, a class that implements this interface gets instantiated and executed
 * by Twister2.
 */
public interface IWorker {
  /**
   * Execute with the resources configured
   *
   * @param config configuration
   * @param workerID the worker id
   * @param allocatedResources allocated resource details
   * @param workerController the worker controller
   * @param persistentVolume information about persistent file system
   * @param volatileVolume information about volatile file system
   */
  void execute(Config config,
               int workerID,
               AllocatedResources allocatedResources,
               IWorkerController workerController,
               IPersistentVolume persistentVolume,
               IVolatileVolume volatileVolume);
}
