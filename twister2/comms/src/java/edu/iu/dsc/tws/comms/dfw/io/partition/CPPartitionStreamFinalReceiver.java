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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;

public class CPPartitionStreamFinalReceiver implements MessageReceiver {
  // the incoming barriers from different sources are stored here
  private Map<Integer, Map<Integer, List<Object>>> incomingBarriers = new HashMap<>();

  // messages before we have seen a barrier
  private Map<Integer, List<Queue<Object>>> messages = new HashMap<>();

  // we put messages to this one after we get a barrier
  private Map<Integer, Queue<Object>> barriers = new HashMap<>();

  // the receiver
  private BulkReceiver receiver;

  private Map<Integer, List<Integer>> expIds;

  public CPPartitionStreamFinalReceiver(BulkReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    this.expIds = expectedIds;
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      messages.put(e.getKey(), new ArrayList<>());
      barriers.put(e.getKey(), new LinkedBlockingQueue<>());
      HashMap<Integer, List<Object>> value = new HashMap<>();
      for (int source : e.getValue()) {
        value.put(source, new ArrayList<>());
      }
      incomingBarriers.put(e.getKey(), value);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
//    Map<Integer, Queue<Object>> barriersForTarget = barriers.get(target);
//    // if this is a barrier lets add it to this targets map of barriers from different sources
//    if ((flags & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
//      barriersForTarget.putIfAbsent(source, true);
//    } else {
//      // we don't have a barrier from this source, we continue to add to messages in before
//      // barrier message map
//      if (!barriersForTarget.containsKey(source)) {
//        messages.get(source).offer(object);
//      } else {
//        afterBarrier.get(source).offer(object);
//      }
//    }
    return true;
  }

  @Override
  public boolean progress() {
    // we always send messages from before barrier map
//    for (Map.Entry<Integer, LinkedBlockingQueue<Object>> e : messages.entrySet()) {
//      Integer target = e.getKey();
//      BlockingQueue<Object> inComingMessages = e.getValue();
//      Object msg = inComingMessages.peek();
//      if (msg != null) {
//        if (msg instanceof List) {
//          boolean offer = receiver.receive(target, ((List<Object>) msg).iterator());
//          if (offer) {
//            inComingMessages.poll();
//          }
//        }
//      }
//      // we have exausted the incoming messages, now lets say this has a barrier ready
//      if (inComingMessages.size() == 0) {
//        Map<Integer, Boolean> barriers = barrierMap.get(target);
//        if (barriers.size() == expIds.get(target).size()) {
//
//        }
//      }
//    }

    return true;
  }
}
