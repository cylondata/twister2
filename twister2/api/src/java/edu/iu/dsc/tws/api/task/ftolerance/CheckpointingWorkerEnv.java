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

import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.ftolerance.api.Snapshot;
import edu.iu.dsc.tws.ftolerance.api.SnapshotImpl;
import edu.iu.dsc.tws.ftolerance.api.StateStore;
import edu.iu.dsc.tws.ftolerance.util.CheckpointUtils;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;
import static edu.iu.dsc.tws.common.config.Context.JOB_NAME;

public final class CheckpointingWorkerEnv {
  private static final Logger LOG = Logger.getLogger(CheckpointingWorkerEnv.class.getName());

  private static final String WORKER_CHECKPOINT_FAMILY = "worker";
  private static final String WORKER_CHECKPOINT_DIR = "twister2-checkpoints";

  private final int workerId;
  private long latestVersion;

  private SnapshotImpl localCheckpoint;
  private StateStore localCheckpointStore;
  private CheckpointingClient checkpointingClient;

  private CheckpointingWorkerEnv(int workerId, long latestVersion, SnapshotImpl localCheckpoint,
                                 StateStore localCheckpointStore,
                                 CheckpointingClient checkpointingClient) {
    this.workerId = workerId;
    this.latestVersion = latestVersion;
    this.localCheckpoint = localCheckpoint;
    this.localCheckpointStore = localCheckpointStore;
    this.checkpointingClient = checkpointingClient;
  }

  public Snapshot getSnapshot() {
    return this.localCheckpoint;
  }

  public void commitSnapshot() {
    // increment current version. There could be an issue if the committing phase fails at this
    // point. todo: Check this!
    latestVersion++;

    // write to disk
    localCheckpoint.setVersion(latestVersion);

    try {
      CheckpointUtils.commitState(localCheckpointStore, WORKER_CHECKPOINT_FAMILY, workerId,
          localCheckpoint, checkpointingClient,
          (id, wId, message) -> LOG.info("Version update received!"));
    } catch (IOException e) {
      throw new RuntimeException("Unable to commit state", e);
    }
  }

  public static Builder newBuilder(Config config, int workerId,
                                   IWorkerController workerController) {
    return new Builder(config, workerId, workerController);
  }


  public static final class Builder {
    private int workerId;
    private Config config;
    private IWorkerController workerController;

    private SnapshotImpl localCheckpoint;

    private Builder(Config config, int workerId, IWorkerController workerController) {
      this.workerId = workerId;
      this.config = config;
      this.workerController = workerController;

      this.localCheckpoint = new SnapshotImpl();
    }

    public Builder registerVariable(String key, DataPacker dataPacker) {
      this.localCheckpoint.setPacker(key, dataPacker);
      return this;
    }

    /**
     * build the checkpointing worker env
     * @return
     */
    public CheckpointingWorkerEnv build() {

      StateStore localCheckpointStore = CheckpointUtils.getStateStore(config);
      // one snapshot store for worker. Each node may have snapshots of multiple  workers
      localCheckpointStore.init(config, WORKER_CHECKPOINT_DIR, config.getStringValue(JOB_NAME),
          Integer.toString(workerId));

      Set<Integer> workerIDs = Collections.emptySet();
      if (workerId == 0) {
        workerIDs = IntStream.range(0, workerController.getNumberOfWorkers()).boxed().
            collect(Collectors.toSet());
      }

      long latestVersion;
      try {
        Checkpoint.FamilyInitializeResponse initFamilyRes =
            workerController.getCheckpointingClient().initFamily(workerId,
                workerController.getNumberOfWorkers(),
                WORKER_CHECKPOINT_FAMILY, workerIDs);

        latestVersion = initFamilyRes.getVersion();
      } catch (BlockingSendException e) {
        throw new RuntimeException("Sending discovery message to checkpoint master failed!", e);
      }

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

      return new CheckpointingWorkerEnv(workerId, latestVersion,
          localCheckpoint, localCheckpointStore, workerController.getCheckpointingClient());
    }
  }


}
