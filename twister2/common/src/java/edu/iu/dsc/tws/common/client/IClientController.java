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
package edu.iu.dsc.tws.common.client;

public interface IClientController {

  /**
   * add new instances of the scalable ComputeResource in the job
   * @param instancesToAdd
   * @return true if successful
   */
  boolean scaleUpComputeResource(int instancesToAdd);

  /**
   * remove some instances of the scalable ComputeResource in the job
   * @param instancesToRemove
   * @return true if successful
   */
  boolean scaleDownComputeResource(int instancesToRemove);

  /**
   * the client can send data to all workers in the job
   * @param data
   * @return
   */
  boolean broadcastToAllWorkers(byte[] data);

}
