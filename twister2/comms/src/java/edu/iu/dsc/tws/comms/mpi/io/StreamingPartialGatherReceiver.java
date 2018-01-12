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
package edu.iu.dsc.tws.comms.mpi.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowGather;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;

public class StreamingPartialGatherReceiver implements MessageReceiver {
  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, List<Object>>> messages = new TreeMap<>();

  private MPIDataFlowGather operation;

  private int sendPendingMax = 128;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    this.operation = (MPIDataFlowGather) op;
    this.sendPendingMax = MPIContext.sendPendingMax(cfg);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayList<Object>());
      }
      messages.put(e.getKey(), messagesPerTask);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d",
          operation.getTaskPlan().getThisExecutor(), target));
    }
    List<Object> messageLists = messages.get(target).get(source);
    if (messageLists.size() > sendPendingMax) {
      canAdd = false;
    } else {
      if (object instanceof MPIMessage) {
        ((MPIMessage) object).incrementRefCount();
      }
      messageLists.add(object);
    }
    return canAdd;
  }

  public void progress() {
    for (int t : messages.keySet()) {
      boolean canProgress = true;
      while (canProgress) {
        // now check weather we have the messages for this source
        Map<Integer, List<Object>> map = messages.get(t);
        boolean found = true;
        for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
            canProgress = false;
          }
        }

        if (found) {
          List<Object> out = new ArrayList<>();
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            Object object = e.getValue().get(0);
            if (!(object instanceof MPIMessage)) {
              out.add(new MultiObject(e.getKey(), object));
            } else {
              out.add(object);
            }
          }
          if (!operation.sendPartial(t, out,
              MPIContext.FLAGS_MULTI_MSG, 0)) {
            canProgress = false;
          }
        }
      }
    }
  }
}
