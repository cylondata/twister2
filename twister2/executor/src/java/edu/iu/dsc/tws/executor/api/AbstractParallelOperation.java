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
package edu.iu.dsc.tws.executor.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public abstract class AbstractParallelOperation implements IParallelOperation {
  protected Config config;

  protected Communicator channel;

  protected Map<Integer, BlockingQueue<IMessage>> outMessages;

  protected TaskPlan taskPlan;

  protected EdgeGenerator edge;

  protected int communicationEdge;

  protected HashMap<Integer, Boolean> barrierMap;

  protected HashMap<Integer, ArrayList<Object>> incommingBuffer;

  protected boolean checkpointStarted = false;


  public AbstractParallelOperation(Config config, Communicator network, TaskPlan tPlan) {
    this.config = config;
    this.taskPlan = tPlan;
    this.channel = network;
    this.outMessages = new HashMap<>();
    this.barrierMap = new HashMap<>();
    this.incommingBuffer = new HashMap<>();

  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return true;
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {
    if (outMessages.containsKey(targetTask)) {
      throw new RuntimeException("Existing queue for target task");
    }
    outMessages.put(targetTask, queue);
  }

  @Override
  public boolean progress() {
    return true;
  }

  public void barrierChecking(
      DataFlowPartition op, int source, int path, int target, int flags, Object object) {
    if ((flags & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
      if (!checkpointStarted) {
        checkpointStarted = true;
      }
      barrierMap.putIfAbsent(source, true);
      if (barrierMap.keySet() == op.getSources()) {
        for (Integer dest :op.getDestinations()) {
          op.send(source, new Object(), MessageFlags.BARRIER, dest);
        }
        //start checkpoint and flush the buffering messages
      }
    } else {
      if (barrierMap.containsKey(source)) {
        if (incommingBuffer.containsKey(source)) {
          incommingBuffer.get(source).add(object);
        } else {
          ArrayList<Object> bufferMessege = new ArrayList<>();
          bufferMessege.add(object);
          incommingBuffer.put(source, bufferMessege);
        }
      } else {
        if (object instanceof List) {
          for (Object o : (List) object) {
            TaskMessage msg = new TaskMessage(o,
                edge.getStringMapping(communicationEdge), target);
            outMessages.get(target).offer(msg);

          }
        }
      }
    }
  }
}
