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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.faulttolerance.JobFaultListener;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.InitBarrierListener;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JobFailureWatcher implements IWorkerFailureListener, InitBarrierListener {
  private static final Logger LOG = Logger.getLogger(JobFailureWatcher.class.getName());

  private JobFaultListener jobFaultListener;

  // job becomes faulty when worker(s) fail
  // after failures, all workers need to be restarted and
  // they all need to arrive at the next init barrier
  private boolean jobFaulty = false;

  // job does not become faulty, if faults occur before proceeding the first barrier
  // todo: we need to set this properly when JM restarted
  private boolean firstInitBarrierProceeded = false;

  public boolean isJobFaulty() {
    return jobFaulty;
  }

  public void setJobFaultListener(JobFaultListener jobFaultListener) {
    this.jobFaultListener = jobFaultListener;
  }

  /**
   * this is only called for INIT barrier
   */
  @Override
  public void allArrived() {
    firstInitBarrierProceeded = true;
    jobFaulty = false;

    if (jobFaultListener != null) {
      LOG.fine("Calling faultRestored()");
      jobFaultListener.faultRestored();
    }
  }

  @Override
  public void failed(int workerID) {
    // ignore fault events if first INIT barrier is not proceeded
    if (!firstInitBarrierProceeded) {
      return;
    }

    jobBecomesFaulty();
  }

  @Override
  public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
    // ignore fault events if first INIT barrier is not proceeded
    if (!firstInitBarrierProceeded) {
      return;
    }

    jobBecomesFaulty();
  }

  private void jobBecomesFaulty() {

    // we need to inform the listener only once for each fault
    // so, we have this condition
    if (!jobFaulty) {
      jobFaulty = true;

      if (jobFaultListener != null) {
        LOG.fine("Calling faultOccurred()");
        jobFaultListener.faultOccurred();
      }
    }
  }
}
