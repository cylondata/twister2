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
package edu.iu.dsc.tws.executor.comms.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.comms.op.stream.SPartition;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

/**
 * The streaming operation.
 */
public class PartitionStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionStreamingOperation.class.getName());

  private boolean checkpointStarted = false;

  protected SPartition op;

  public PartitionStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                     Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                                     DataType dataType, String edgeName) {
    super(config, network, tPlan);
    this.edgeGenerator = e;

    if (srcs.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dests == null) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    op = new SPartition(channel, taskPlan, srcs, dests, Utils.dataTypeToMessageType(dataType),
        new PartitionReceiver(), new LoadBalanceSelector());
    communicationEdge = e.generate(edgeName);
  }

  public boolean send(int source, IMessage message, int flags) {
    return op.partition(source, message.getContent(), flags);
  }

  public class PartitionReceiver implements MessageReceiver {
    private HashMap<Integer, Boolean> barrierMap = new HashMap<>();

    private HashMap<Integer, ArrayList<Object>> incommingBuffer = new HashMap<>();
    // keep track of the incoming messages
    private Map<Integer, BlockingQueue<TaskMessage>> inComing = new HashMap<>();

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        inComing.put(e.getKey(), new LinkedBlockingQueue<>());
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      if ((flags & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
        if (!checkpointStarted) {
          checkpointStarted = true;
        }
        barrierMap.putIfAbsent(source, true);
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
          BlockingQueue<TaskMessage> messages = inComing.get(target);
          if (messages.size() > 128) {
            return false;
          }

          if (object instanceof List) {
            TaskMessage msg = new TaskMessage(object,
                edgeGenerator.getStringMapping(communicationEdge), target);

            messages.offer(msg);
          }
        }
      }
      return true;
    }

    @Override
    public boolean progress() {
      for (Map.Entry<Integer, BlockingQueue<TaskMessage>> e : inComing.entrySet()) {
        BlockingQueue<IMessage> messages = outMessages.get(e.getKey());
        BlockingQueue<TaskMessage> inComingMessages = inComing.get(e.getKey());

        TaskMessage msg = inComingMessages.peek();
        if (msg != null) {
          boolean offer = messages.offer(msg);
          if (offer) {
            inComingMessages.poll();
          }
        }
      }

      return true;
    }
  }

  public boolean progress() {
    return op.progress();
  }
}
