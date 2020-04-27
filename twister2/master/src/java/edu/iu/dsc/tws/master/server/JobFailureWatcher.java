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
package edu.iu.dsc.tws.master.server;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.faulttolerance.JobFaultListener;
import edu.iu.dsc.tws.api.resource.IBarrierListener;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JobFailureWatcher implements IWorkerFailureListener, IBarrierListener {

  private JobFaultListener jobFaultListener;

  // i keep workerIDs as String
  // not to perform accidentally index based operations in the list such as remove
  private List<String> failedWorkers = new ArrayList<>();

  // job becomes faulty when worker(s) fail
  // after failures, all workers need to be restarted and they all arrive at the next barrier
  private boolean jobFaulty = false;

  public JobFailureWatcher(JobFaultListener jobFaultListener) {
    this.jobFaultListener = jobFaultListener;
  }

  @Override
  public void allArrived() {
    if (jobFaultListener == null) {
      return;
    }

    if (jobFaulty && failedWorkers.isEmpty()) {
      jobFaulty = false;
      jobFaultListener.faultRestored();
    }
  }

  @Override
  public void failed(int workerID) {
    if (jobFaultListener == null) {
      return;
    }

    if (!jobFaulty) {
      jobBecomesFaulty();
    }

    failedWorkers.add(Integer.toString(workerID));
  }

  @Override
  public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
    if (jobFaultListener == null) {
      return;
    }

    if (!jobFaulty) {
      jobBecomesFaulty();
    }

    failedWorkers.remove(Integer.toString(workerInfo.getWorkerID()));
  }

  private void jobBecomesFaulty() {
    jobFaulty = true;
    jobFaultListener.faultOccurred();
  }
}
