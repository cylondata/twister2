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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public class PartitionStreamingFinalReceiver implements MessageReceiver {
  // messages before we have seen a barrier
  private Map<Integer, Queue<Object>> messages = new HashMap<>();

  // the receiver
  private SingularReceiver receiver;

  // the receiving indexes for the target
  private Map<Integer, Integer> receivingIndexes = new HashMap<>();

  public PartitionStreamingFinalReceiver(SingularReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    int sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      messages.put(e.getKey(), new ArrayBlockingQueue<>(sendPendingMax));
      receivingIndexes.put(e.getKey(), 0);
    }
    this.receiver.init(cfg, expectedIds.keySet());
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    return messages.get(target).offer(object);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    // we always send messages from before barrier map
    for (Map.Entry<Integer, Queue<Object>> e : messages.entrySet()) {
      Integer target = e.getKey();
      Queue<Object> inComingMessages = e.getValue();
      Object msg = inComingMessages.peek();

      if (msg != null && msg instanceof List) {
        int startIndex = receivingIndexes.get(e.getKey());
        for (int i = startIndex; i < ((List) msg).size(); i++) {
          boolean offer = receiver.receive(target, ((List) msg).get(i));
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
        boolean offer = receiver.receive(target, msg);
        if (offer) {
          inComingMessages.poll();
        } else {
          needsFurtherProgress = true;
        }
      }
    }
    return needsFurtherProgress;
  }
}
