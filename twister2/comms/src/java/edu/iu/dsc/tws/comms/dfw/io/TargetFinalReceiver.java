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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.config.Config;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public abstract class TargetFinalReceiver extends TargetReceiver {
  private static final Logger LOG = Logger.getLogger(TargetFinalReceiver.class.getName());
  /**
   * Keep weather we have received a sync from a source
   */
  protected Map<Integer, Set<Integer>> syncReceived = new HashMap<>();

  /**
   * Keep state about the targets
   */
  protected Int2ObjectOpenHashMap<ReceiverState> targetStates = new Int2ObjectOpenHashMap<>();

  /**
   * The barriers for each target
   */
  protected Int2ObjectOpenHashMap<byte[]> barriers = new Int2ObjectOpenHashMap<>();

  /**
   * State is cleared
   */
  protected boolean stateCleared = false;

  protected boolean calledReceive = false;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = op.getSources();
    thisDestinations = expectedIds.keySet();

    for (Integer target : expectedIds.keySet()) {
      syncReceived.put(target, new HashSet<>());
      targetStates.put(target, ReceiverState.INIT);
    }

    int index = 0;
    targets = new int[thisDestinations.size()];
    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
      targets[index++] = target;
    }
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    Set<Integer> sources = syncReceived.get(target);
    sources.add(source);
    for (int t : thisDestinations) {
      Set<Integer> syncSources = syncReceived.get(t);
      if (syncSources.equals(this.thisSources)) {
        targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
      }
    }
  }

  @Override
  protected void addSyncMessageBarrier(int source, int target, byte[] barrier) {
    Set<Integer> sources = syncReceived.get(target);
    sources.add(source);
    for (int t : thisDestinations) {
      Set<Integer> syncSources = syncReceived.get(t);
      if (syncSources.equals(this.thisSources)) {
        targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
      }
    }
    syncState = SyncState.BARRIER_SYNC;
    barriers.put(target, barrier);
  }

  @Override
  protected boolean canAcceptMessage(int source, int target) {
    Set<Integer> sources = syncReceived.get(target);
    if (sources.contains(source)) {
      return false;
    }

    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        || targetStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    if (targetStates.get(target) == ReceiverState.INIT) {
      targetStates.put(target, ReceiverState.RECEIVING);
    }

    Queue<Object> msgQueue = messages.get(target);
    return msgQueue.size() < highWaterMark;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;

    lock.lock();
    try {
      for (int i = 0; i < targets.length; i++) {
        int key = targets[i];
        Queue<Object> val = messages.get(key);

        if (val.size() > 0) {
          merge(key, val);
        }

        // check weather we are ready to send and we have values to send
        if (!isFilledToSend(key)) {
          continue;
        }

        // if we send this list successfully
        if (!sendToTarget(representSource, key)) {
          needsFurtherProgress = true;
        }

        if (!val.isEmpty() || !isAllEmpty(key) || !sync(key)) {
          needsFurtherProgress = true;
        }
      }
    } finally {
      lock.unlock();
    }

    return needsFurtherProgress;
  }

  protected abstract boolean isAllEmpty(int target);

  protected boolean sync(int target) {
    boolean allSynced = true;
    // if we have synced no need to go forward
    if (targetStates.get(target) == ReceiverState.INIT
        || targetStates.get(target) == ReceiverState.SYNCED) {
      return allSynced;
    }

    if (targetStates.get(target) == ReceiverState.RECEIVING) {
      allSynced = false;
    }

    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED) {
      targetStates.put(target, ReceiverState.SYNCED);
      onSyncEvent(target, barriers.get(target));
    }

    return allSynced;
  }

  @Override
  public void clean() {
    for (int taraget : targetStates.keySet()) {
      clearTarget(taraget);

      targetStates.put(taraget, ReceiverState.INIT);
    }

    for (Map.Entry<Integer, Set<Integer>> e : syncReceived.entrySet()) {
      e.getValue().clear();
    }

    barriers.clear();
    stateCleared = false;
  }
}
