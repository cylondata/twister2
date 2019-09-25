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
package edu.iu.dsc.tws.local.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class MockWorkerController implements IWorkerController {

  private Twister2Job twister2Job;
  private Config config;
  private final int workerId;
  private CyclicBarrier cyclicBarrier;

  public MockWorkerController(Twister2Job twister2Job,
                              Config config, int workerId,
                              CyclicBarrier cyclicBarrier) {
    this.twister2Job = twister2Job;
    this.config = config;
    this.workerId = workerId;
    this.cyclicBarrier = cyclicBarrier;
  }


  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return this.getWorkerInfoForID(this.workerId);
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
    return JobMasterAPI.WorkerInfo.newBuilder()
        .setWorkerIP("localhost")
        .setPort(8000 + id)
        .setWorkerID(id)
        .setNodeInfo(
            JobMasterAPI.NodeInfo.newBuilder()
                .setNodeIP("localhost")
                .setDataCenterName("dc")
                .setRackName("rck")
                .build()
        )
        .build();
  }

  @Override
  public int getNumberOfWorkers() {
    return this.twister2Job.getNumberOfWorkers();
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getJoinedWorkers() {
    return this.getAllWorkers();
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getAllWorkers() {
    List<JobMasterAPI.WorkerInfo> workers = new ArrayList<>();
    for (int i = 0; i < this.twister2Job.getNumberOfWorkers(); i++) {
      workers.add(this.getWorkerInfoForID(i));
    }
    return workers;
  }

  @Override
  public void waitOnBarrier() throws TimeoutException {
    try {
      this.cyclicBarrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      throw new TimeoutException("Timeout on barrier");
    }
  }

  @Override
  public boolean updateWorkerStatus(JobMasterAPI.WorkerState newState) {
    return false;
  }

  @Override
  public JobMasterAPI.WorkerState getWorkerStatusForID(int id) {
    return null;
  }

}
