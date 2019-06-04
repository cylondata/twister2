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
package edu.iu.dsc.tws.executor.core.streaming;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.api.StateStore;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.core.TaskCheckpointUtils;

public class PendingCheckpoint {

  private static final Logger LOG = Logger.getLogger(PendingCheckpoint.class.getName());

  private boolean pending;
  private CheckpointableTask checkpointableTask;
  private int globalTaskId;
  private int noOfedges;
  private CheckpointingClient checkpointingClient;
  private String taskGraphName;
  private StateStore stateStore;
  private SnapshotImpl snapshot;
  private IParallelOperation[] streamingInParOps;

  private long currentBarrierId;
  private Set<String> edgesOnCurrentBarrier;


  public PendingCheckpoint(String taskGraphName,
                           CheckpointableTask checkpointableTask,
                           int globalTaskId,
                           IParallelOperation[] streamingInParOps,
                           int noOfedges,
                           CheckpointingClient checkpointingClient,
                           StateStore stateStore,
                           SnapshotImpl snapshot) {
    this.checkpointableTask = checkpointableTask;
    this.globalTaskId = globalTaskId;
    this.streamingInParOps = streamingInParOps;
    this.noOfedges = noOfedges;
    this.checkpointingClient = checkpointingClient;
    this.taskGraphName = taskGraphName;
    this.stateStore = stateStore;
    this.snapshot = snapshot;
    this.edgesOnCurrentBarrier = new HashSet<>();
  }

  public void schedule(String edge, long barrierId) {
    if (!pending) {
      pending = true;
      this.currentBarrierId = barrierId;
    }

    if (this.currentBarrierId != barrierId) {
      LOG.severe("Barrier ID mismatch. Expected " + this.currentBarrierId
          + ", received " + barrierId);
    }
    this.edgesOnCurrentBarrier.add(edge);
  }

  public boolean isPending() {
    return pending;
  }

  private void reset() {
    for (int i = 0; i < this.streamingInParOps.length; i++) {
      streamingInParOps[i].reset();
    }
    this.edgesOnCurrentBarrier.clear();
    this.pending = false;
    this.currentBarrierId = -1;
  }

  public long execute() {
    if (!this.pending) {
      return -1;
    }

    if (this.edgesOnCurrentBarrier.size() == this.noOfedges) {
      LOG.fine(() -> "Barrier executing in " + this.globalTaskId + " with id "
          + this.currentBarrierId);
      TaskCheckpointUtils.checkpoint(
          this.currentBarrierId,
          this.checkpointableTask,
          this.snapshot,
          this.stateStore,
          this.taskGraphName,
          this.globalTaskId,
          this.checkpointingClient
      );
      long barrierId = this.currentBarrierId;
      this.reset();
      return barrierId;
    } else {
      LOG.warning("Called checkpoint execute when barriers "
          + "are partially received from sources.");
    }
    return -1;
  }
}
