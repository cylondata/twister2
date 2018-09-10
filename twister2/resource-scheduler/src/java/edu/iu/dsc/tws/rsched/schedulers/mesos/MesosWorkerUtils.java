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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class MesosWorkerUtils {
  private static final Logger LOG = Logger.getLogger(MesosWorkerUtils.class.getName());

  private MesosWorkerUtils() {

  }

  public static AllocatedResources createAllocatedResources(String cluster,
                                                            int workerID,
                                                            JobAPI.Job job) {
    JobAPI.WorkerComputeResource computeResource =
        job.getJobResources().getResources(0).getWorkerComputeResource();

    AllocatedResources allocatedResources = new AllocatedResources(cluster, workerID);
    LOG.info("job get number of workers....:" + job.getNumberOfWorkers());
    for (int i = 0; i < job.getNumberOfWorkers(); i++) {
      allocatedResources.addWorkerComputeResource(new WorkerComputeResource(
          i, computeResource.getCpu(), computeResource.getRam(), computeResource.getDisk()));
    }
    return allocatedResources;
  }
}
