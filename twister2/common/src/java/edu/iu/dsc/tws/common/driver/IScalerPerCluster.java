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
package edu.iu.dsc.tws.common.driver;

/**
 * An interface that scales up/down the number of workers in a Twister2 job
 * in a specific cluster
 * An implementtaion of this interface will be written for each cluster
 * such as Kubernetes, Mesos, Slurm, Nomad, etc
 */

public interface IScalerPerCluster {

  /**
   * whether this job is scalable
   * @return true if scalable
   */
  boolean isScalable();

  /**
   * add new instances of workers to the job
   * @param instancesToAdd
   * @return true if successful
   */
  boolean scaleUpWorkers(int instancesToAdd);

  /**
   * remove some instances of the workers from the job
   * @param instancesToRemove
   * @return true if successful
   */
  boolean scaleDownWorkers(int instancesToRemove);

}
