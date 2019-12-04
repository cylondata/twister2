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
package edu.iu.dsc.tws.proto.utils;

import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class JobUtils {

  private JobUtils() { }

  /**
   * update numberOfWorkers and instances in scalable ComputeResource
   * we assume the last ComputeResource is scalable
   * workerChange is negative when the job is scaled down,
   * it is positive when it is scaled up
   * @param job
   * @param workerChange
   * @return
   */
  public static JobAPI.Job scaleJob(JobAPI.Job job, int workerChange) {
    int newNumberOfWorkers = job.getNumberOfWorkers() + workerChange;

    int scalableCompResIndex = job.getComputeResourceCount() - 1;
    JobAPI.ComputeResource scalableCompRes = job.getComputeResource(scalableCompResIndex);
    int podChange = workerChange / scalableCompRes.getWorkersPerPod();
    int newPodInstances = scalableCompRes.getInstances() + podChange;
    scalableCompRes = scalableCompRes.toBuilder().setInstances(newPodInstances).build();

    return job.toBuilder()
        .setNumberOfWorkers(newNumberOfWorkers)
        .removeComputeResource(scalableCompResIndex)
        .addComputeResource(scalableCompRes)
        .build();
  }

}
