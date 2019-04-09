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
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public abstract class TargetPartialReceiver extends TargetSyncReceiver {
  private static final Logger LOG = Logger.getLogger(TargetPartialReceiver.class.getName());
  /**
   * Keep state about the targets
   */
  protected Map<Integer, ReceiverState> sourceStates = new HashMap<>();

  /**
   * Keep track what are the targets we've sent syncs to
   */
  protected Map<Integer, Set<Integer>> syncSent = new HashMap<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = TaskPlanUtils.getTasksOfThisWorker(op.getTaskPlan(), op.getSources());
    thisDestinations = op.getTargets();
    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
    }
    // we are at the receiving state
    for (int source : thisSources) {
      sourceStates.put(source, ReceiverState.RECEIVING);
      syncSent.put(source, new HashSet<>());
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    boolean b = super.onMessage(source, path, target, flags, object);
    LOG.info("Counts : " + counts);
    return b;
  }

  @Override
  protected boolean sendToTarget(int source, int target, List<Object> values) {
    return operation.sendPartial(source, values, 0, target);
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return readyToSend.get(target) != null && readyToSend.get(target).size() > 0;
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    sourceStates.put(source, ReceiverState.ALL_SYNCS_RECEIVED);
  }

  @Override
  protected boolean canAcceptMessage(int source, int target) {
    if (sourceStates.get(source) == ReceiverState.ALL_SYNCS_RECEIVED
        || sourceStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    Queue<Object> msgQueue = messages.get(target);
    return msgQueue.size() < highWaterMark;
  }

  @Override
  public void onFinish(int source) {
    addSyncMessage(source, 0);
  }

  @Override
  public boolean sync() {
    boolean allSynced = true;

    for (Map.Entry<Integer, ReceiverState> e : sourceStates.entrySet()) {
      if (e.getValue() == ReceiverState.RECEIVING) {
        allSynced = false;
        continue;
      }

      // if we have synced no need to go forward
      if (e.getValue() == ReceiverState.SYNCED) {
        continue;
      }

      for (int source : thisSources) {
        Set<Integer> finishedDestPerSource = syncSent.get(source);
        for (int dest : thisDestinations) {
          if (!finishedDestPerSource.contains(dest)) {
            if (operation.sendPartial(source, new Byte[1], MessageFlags.END, dest)) {
              finishedDestPerSource.add(dest);

              if (finishedDestPerSource.size() == thisDestinations.size()) {
                sourceStates.put(source, ReceiverState.SYNCED);
              }
            } else {

              allSynced = false;
              // no point in going further
              break;
            }
          }
        }
      }
    }

    return allSynced;
  }
}
