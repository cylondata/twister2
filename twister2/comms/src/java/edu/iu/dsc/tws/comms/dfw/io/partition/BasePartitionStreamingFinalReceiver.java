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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class BasePartitionStreamingFinalReceiver {
  private static final Logger LOG = Logger.getLogger(
      BasePartitionStreamingFinalReceiver.class.getName());

  // messages before we have seen a barrier
  private Map<Integer, Queue<Object>> messages = new HashMap<>();

  // the receiving indexes for the target
  private Map<Integer, Integer> receivingIndexes = new ConcurrentHashMap<>();

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  private Lock lock = new ReentrantLock();

  private Map<Integer, Integer> counts = new HashMap<>();

  private int workerId;

  public BasePartitionStreamingFinalReceiver() {
  }

  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    int sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    workerId = operation.getTaskPlan().getThisExecutor();
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      messages.put(e.getKey(), new ArrayBlockingQueue<>(sendPendingMax));
      receivingIndexes.put(e.getKey(), 0);
      counts.put(e.getKey(), 0);
    }
  }

  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    lock.lock();
    try {
      if ((flags & MessageFlags.END) == MessageFlags.END) {
        return true;
      }

      int count = counts.get(target);
      if (object instanceof List) {
        counts.put(target, ((List) object).size() + count);
      } else {
        counts.put(target, 1 + count);
      }

      return messages.get(target).offer(object);
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean progress() {
    lock.lock();
    try {
      boolean needsFurtherProgress = false;
      // we always send messages from before barrier map
      for (Map.Entry<Integer, Queue<Object>> e : messages.entrySet()) {
        Integer target = e.getKey();
        Queue<Object> inComingMessages = e.getValue();
        Object msg = inComingMessages.peek();

        if (msg instanceof List) {
          int startIndex = receivingIndexes.get(e.getKey());
          for (int i = startIndex; i < ((List) msg).size(); i++) {
            boolean offer = receive(target, ((List) msg).get(i));
            if (offer) {
              receivingIndexes.put(e.getKey(), i + 1);
            } else {
              needsFurtherProgress = true;
            }
          }
          // we have to reset to 0
          if (startIndex == ((List) msg).size()) {
            inComingMessages.poll();
            receivingIndexes.put(e.getKey(), 0);
          }
        } else if (msg != null) {
          boolean offer = receive(target, msg);
          if (offer) {
            inComingMessages.poll();
          } else {
            needsFurtherProgress = true;
          }
        }
      }
      LOG.info(String.format("%d COUNTS %s", workerId, counts));
      return needsFurtherProgress;
    } finally {
      lock.unlock();
    }
  }

  public abstract boolean receive(int target, Object message);
}
