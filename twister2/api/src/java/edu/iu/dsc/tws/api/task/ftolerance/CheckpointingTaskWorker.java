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
package edu.iu.dsc.tws.api.task.ftolerance;

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.ftolerance.api.CheckpointingContext;
import edu.iu.dsc.tws.ftolerance.api.Snapshot;
import edu.iu.dsc.tws.ftolerance.api.SnapshotImpl;
import edu.iu.dsc.tws.ftolerance.api.StateStore;

public abstract class CheckpointingTaskWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(CheckpointingTaskWorker.class.getName());

  private SnapshotImpl localSnapshot;
  private StateStore localSnapshotStore;

  private boolean recovering = false;

  private void init(){
    try {
      this.localSnapshotStore = ReflectionUtils.newInstance(CheckpointingContext.checkpointStoreClass(config));
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate snapshot store", e);
    }
    localSnapshotStore.init(config, "worker_" + workerId);
    // note: one snapshot store for worker. Each worker may have snapshots of multiple  tasks

    // todo take job id from config
    String jobId = "some_job_" + workerId + "_";
    this.localSnapshot = new SnapshotImpl(jobId);
  }

  @Override
  public void execute() {
    LOG.info("Initializing CheckpointingTaskWorker ");
    init();

    LOG.info("Preparing the checkpoint");
    prepare(localSnapshot);

    // if job in recovery mode, get the localSnapshot ID from the job and unpack the localSnapshot
    String checkpointID = CheckpointingContext.restoreCheckpoint(config);
    if (!checkpointID.isEmpty()) {
      LOG.info("Restore checkpoint set. Starting from checkpoint: " + checkpointID);
      try {
        localSnapshot.unpack(localSnapshotStore.get(checkpointID));
      } catch (IOException e) {
        throw new RuntimeException("Unable to unpack the checkpoint " + checkpointID, e);
      }
    } else {
      LOG.info("No checkpoints to restore.");
    }

    LOG.info("Executing checkpointed worker");
    execute(localSnapshot);
  }

  public abstract void execute(Snapshot snapshot);

  public abstract void prepare(Snapshot snapshot);

  public void commitSnapshot() {
    long version = localSnapshot.getVersion();


    // todo: add a broadcast
  }


  private String generateCheckpointID(String jobID, long version) {
    return jobID + "_" + version;
  }
}
