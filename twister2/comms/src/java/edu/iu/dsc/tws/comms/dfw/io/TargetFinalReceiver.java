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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

public abstract class TargetFinalReceiver extends TargetReceiver {
  private static final Logger LOG = Logger.getLogger(TargetFinalReceiver.class.getName());
  /**
   * Keep weather we have received a sync from a source
   */
  protected Map<Integer, Set<Integer>> syncReceived = new HashMap<>();

  /**
   * Keep state about the targets
   */
  protected Map<Integer, ReceiverState> targetStates = new HashMap<>();

  /**
   * State is cleared
   */
  protected boolean stateCleared = false;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = op.getSources();
    thisDestinations = expectedIds.keySet();

    for (Integer target : expectedIds.keySet()) {
      syncReceived.put(target, new HashSet<>());
      targetStates.put(target, ReceiverState.RECEIVING);
    }

    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
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
  protected boolean canAcceptMessage(int source, int target) {
    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        || targetStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    Queue<Object> msgQueue = messages.get(target);
    return msgQueue.size() < highWaterMark;
  }

  @Override
  protected boolean sync() {
    boolean allSynced = true;
    for (int target : thisDestinations) {
      if (targetStates.get(target) == ReceiverState.RECEIVING) {
        allSynced = false;
      }

      if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED) {
        onSyncEvent(target);
        targetStates.put(target, ReceiverState.SYNCED);
      }
    }

    if (allSynced && !stateCleared) {
      for (int t : thisDestinations) {
        clearTarget(t);
      }
      for (Map.Entry<Integer, Set<Integer>> e : syncReceived.entrySet()) {
        e.getValue().clear();
      }
      stateCleared = true;
    }

    return allSynced;
  }
}
