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
package edu.iu.dsc.tws.executor.core;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;

public final class TaskCheckpointUtils {

  private static final Logger LOG = Logger.getLogger(TaskCheckpointUtils.class.getName());

  private TaskCheckpointUtils() {

  }

  public static void restore(CheckpointableTask checkpointableTask,
                             SnapshotImpl snapshot,
                             StateStore stateStore, long tasksVersion, int globalTaskId) {
    checkpointableTask.initSnapshot(snapshot);
    if (tasksVersion > 0) {
      try {
        CheckpointUtils.restoreSnapshot(stateStore,
            tasksVersion,
            snapshot);
        LOG.log(Level.FINE, "Restoring task " + globalTaskId + " to version " + tasksVersion);
        checkpointableTask.restoreSnapshot(snapshot);
      } catch (IOException e) {
        throw new RuntimeException("Failed to restore snapshot of " + globalTaskId, e);
      }
    }
  }

  public static void checkpoint(long checkpointID,
                                CheckpointableTask checkpointableTask,
                                SnapshotImpl snapshot,
                                StateStore stateStore,
                                String family,
                                int globalTaskId,
                                CheckpointingClient checkpointingClient) {
    try {
      //take the task snapshot
      checkpointableTask.takeSnapshot(snapshot);

      //update the new version
      snapshot.setVersion(checkpointID);

      CheckpointUtils.commitState(stateStore,
          family,
          globalTaskId,
          snapshot,
          checkpointingClient,
          (id, wid, msg) -> {
            LOG.log(Level.FINE, "Checkpoint of " + globalTaskId
                + " committed with version : " + checkpointID);
          }
      );
      checkpointableTask.onSnapshotPersisted(snapshot);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write checkpoint of " + globalTaskId, e);
    }
  }
}
