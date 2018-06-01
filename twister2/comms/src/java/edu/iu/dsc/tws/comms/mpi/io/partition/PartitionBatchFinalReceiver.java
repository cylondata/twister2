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
package edu.iu.dsc.tws.comms.mpi.io.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;

public class PartitionBatchFinalReceiver implements MessageReceiver {
  private Map<Integer, Map<Integer, Boolean>> finished;

  private Map<Integer, Map<Integer, List<Object>>> data;

  private int messageCount = 0;

  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    finished = new ConcurrentHashMap<>();
    for (Integer integer : expectedIds.keySet()) {
      Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
      Map<Integer, List<Object>> d = new ConcurrentHashMap<>();
      for (Integer exp : expectedIds.get(integer)) {
        perTarget.put(exp, false);
        d.put(exp, new ArrayList<>());
      }
      finished.put(integer, perTarget);
    }
  }

  @Override
  public boolean onMessage(int source, int destination, int target, int flags, Object object) {
    // add the object to the map
    messageCount++;
    if ((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) {
      finished.get(target).put(source, true);
    }

    if (((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) && isAllFinished(target)) {
      System.out.println(target + " : : " + Arrays.toString((byte[]) object));
      System.out.printf("All Done for Task %d \n", target);
    }
    System.out.println("Task : " + target + " Message Count :" + source);
    return true;
  }

  @Override
  public void progress() {

  }

  private boolean isAllFinished(int target) {
    boolean isDone = true;
    for (Boolean bol : finished.get(target).values()) {
      isDone &= bol;
    }
    return isDone;
  }


}
