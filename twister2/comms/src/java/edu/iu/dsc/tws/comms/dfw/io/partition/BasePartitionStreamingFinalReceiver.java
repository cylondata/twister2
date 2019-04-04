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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

public abstract class BasePartitionStreamingFinalReceiver {
  private static final Logger LOG = Logger.getLogger(
      BasePartitionStreamingFinalReceiver.class.getName());

  // messages before we have seen a barrier
  private Map<Integer, Queue<Object>> messages = new HashMap<>();

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  private Lock lock = new ReentrantLock();


  public BasePartitionStreamingFinalReceiver() {
  }

  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      messages.put(e.getKey(), new LinkedBlockingQueue<>());
    }
  }

  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    lock.lock();
    try {
      if ((flags & MessageFlags.END) == MessageFlags.END) {
        return true;
      }

      if (object instanceof List) {
        messages.get(target).addAll((Collection<?>) object);
      } else {
        messages.get(target).add(object);
      }
    } finally {
      lock.unlock();
    }
    return true;
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

        Object data = inComingMessages.peek();
        if (data != null) {
          boolean offer = receive(target, data);
          if (offer) {
            inComingMessages.poll();
          } else {
            needsFurtherProgress = true;
          }
        }
      }
      return needsFurtherProgress;
    } finally {
      lock.unlock();
    }
  }

  public abstract boolean receive(int target, Object message);
}
