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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.exceptions.JobFaultyException;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

import mpi.MPI;
import mpi.MPIException;

public class MPIWorkerController implements IWorkerController {

  private static final Logger LOG = Logger.getLogger(MPIWorkerController.class.getName());

  private int workerID;
  private int restartCount;

  private Map<Integer, JobMasterAPI.WorkerInfo> workerInfoMap = new HashMap<>();

  public MPIWorkerController(int workerID,
                             Map<Integer, JobMasterAPI.WorkerInfo> workers,
                             int restartCount) {
    this.workerID = workerID;
    this.workerInfoMap = workers;
    this.restartCount = restartCount;
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return workerInfoMap.get(workerID);
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
    return workerInfoMap.get(id);
  }

  @Override
  public int getNumberOfWorkers() {
    return workerInfoMap.size();
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getJoinedWorkers() {
    return new ArrayList<>(workerInfoMap.values());
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getAllWorkers() throws TimeoutException {
    return new ArrayList<>(workerInfoMap.values());
  }

  @Override
  public int workerRestartCount() {
    return restartCount;
  }

  @Override
  public void waitOnBarrier() throws TimeoutException {
    try {
      MPI.COMM_WORLD.barrier();
    } catch (MPIException e) {
      throw new Twister2RuntimeException("Failed to wait on barrier");
    }
  }

  @Override
  public void waitOnBarrier(long timeLimit) throws TimeoutException, JobFaultyException {
    waitOnBarrier();
  }

  @Override
  public void waitOnInitBarrier() throws TimeoutException {
    waitOnBarrier();
  }

  @Override
  public CheckpointingClient getCheckpointingClient() {
    return null;
  }

  public boolean addAllJoinedListener(IAllJoinedListener iAllJoinedListener) {

    // deliver worker list in a thread
    new Thread("Twister2-MPIWorkerController-AllJoinedSupplier") {
      @Override
      public void run() {
        try {
          iAllJoinedListener.allWorkersJoined(getAllWorkers());
        } catch (TimeoutException e) {
          LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        LOG.fine("AllWorkersJoined event delivered from cache.");
      }
    }.start();

    return true;
  }
}
