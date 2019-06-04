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
import java.util.Collections;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.checkpointing.api.CheckpointingContext;
import edu.iu.dsc.tws.checkpointing.api.Snapshot;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.api.StateStore;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;
import static edu.iu.dsc.tws.common.config.Context.JOB_ID;

public abstract class CheckpointingTaskWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(CheckpointingTaskWorker.class.getName());

  private static final String WORKER_CHECKPOINT_FAMILY = "worker";
  private SnapshotImpl localCheckpoint;
  private StateStore localCheckpointStore;
  private long latestVersion = 0L;

  private CheckpointingClient checkpointClient;

  /**
   * initializes checkpoint store, checkpoint obj and checkpoint client and retrieves the
   * checkpoint version from the checkpoint mgr
   */
  private void init() {

    try {
      this.localCheckpointStore = ReflectionUtils.newInstance(CheckpointingContext.
          checkpointStoreClass(config));
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate snapshot store", e);
    }
    localCheckpointStore.init(config, config.getStringValue(JOB_ID),
        Integer.toString(workerId));
    // note: one snapshot store for worker. Each worker may have snapshots of multiple  tasks

    this.localCheckpoint = new SnapshotImpl();

    this.checkpointClient = workerController.getCheckpointingClient();

    Set<Integer> workerIDs;
    if (workerId == 0) {
      LOG.fine("Updating the worker list in the checkpoint manager");
      workerIDs = IntStream.range(0, workerController.getNumberOfWorkers()).boxed().
          collect(Collectors.toSet());
    } else {
      workerIDs = Collections.emptySet();
    }

    Checkpoint.FamilyInitializeResponse initFamilyRes;
    try {
      initFamilyRes = checkpointClient.initFamily(workerId, workerController.getNumberOfWorkers(),
          WORKER_CHECKPOINT_FAMILY, workerIDs);
      workerController.waitOnBarrier();
    } catch (BlockingSendException | TimeoutException e) {
      throw new RuntimeException("Sending discovery message to checkpoint master failed!", e);
    }

    this.latestVersion = initFamilyRes.getVersion();
    // note: local checkpoint is not recovered as yet! it will be recovered with the unpacking of
    // the recovering checkpoint
  }

  /**
   * Users can set packers for checkpointing variables here. Setting type specific variables
   * would make the ser-de processes more efficient
   *
   * @param checkpoint local checkpoint
   */
  public abstract void prepare(Snapshot checkpoint);

  /**
   * Execution with checkpointing
   *
   * @param checkpoint local checkpoint
   */
  public abstract void execute(Snapshot checkpoint);

  /**
   * Users need to call this method to commit the checkpointed variables. This would increment
   * the current version, write the checkpoint to the disk and update the version in the
   * checkpoint manager
   */
  protected void commitCheckpoint() {
    // increment current version. There could be an issue if the committing phase fails at this
    // point. todo: Check this!
    latestVersion++;

    // write to disk
    localCheckpoint.setVersion(latestVersion);

    try {
      CheckpointUtils.commitState(localCheckpointStore, WORKER_CHECKPOINT_FAMILY, workerId,
          localCheckpoint, checkpointClient,
          (id, workerId, message) -> LOG.info("Version update received!"));
    } catch (IOException e) {
      throw new RuntimeException("Unable to commit state", e);
    }

  }

  @Override
  public final void execute() {
    LOG.info("Initializing CheckpointingTaskWorker ");
    init();

    LOG.info("Preparing the checkpoint");
    prepare(localCheckpoint);

    if (latestVersion > 0) {
      LOG.info("Restore checkpoint set. Starting from checkpoint: " + latestVersion);
      try {
        CheckpointUtils.restoreSnapshot(localCheckpointStore, latestVersion, localCheckpoint);
      } catch (IOException e) {
        throw new RuntimeException("Unable to unpack the checkpoint " + latestVersion, e);
      }
    } else {
      LOG.info("No checkpoints to recover. Starting fresh");
    }

    LOG.info("Executing CheckpointingTaskWorker");
    execute(localCheckpoint);
  }
}
