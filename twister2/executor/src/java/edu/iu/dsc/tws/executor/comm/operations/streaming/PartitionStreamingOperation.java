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
package edu.iu.dsc.tws.executor.comm.operations.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;


import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.LoadBalanceDestinationSelector;
import edu.iu.dsc.tws.comms.op.stream.SPartition;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionStreamingOperation.class.getName());
  private HashMap<Integer, Boolean> barrierMap = new HashMap<>();

  private HashMap<Integer, ArrayList<Object>> incommingBuffer = new HashMap<>();
  private boolean checkpointStarted = false;

  protected SPartition partition;
  private Communicator communicator;
  private TaskPlan taskPlan;



  public PartitionStreamingOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
    this.communicator = new Communicator(config, network);
    this.taskPlan = tPlan;
  }

  public void prepare(Set<Integer> sources, Set<Integer> destinations, EdgeGenerator e,
                      MessageType dataType, String edgeName) {
    this.partition = new SPartition(communicator, taskPlan, sources, destinations, dataType,
        new PartitionReceiver(), new LoadBalanceDestinationSelector());
  }

  public void prepare(Set<Integer> sources, Set<Integer> destinations, EdgeGenerator e,
                      MessageType dataType, MessageType keyType, String edgeName) {
    this.partition = new SPartition(communicator, taskPlan, sources, destinations, dataType,
        new PartitionReceiver(), new LoadBalanceDestinationSelector());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return partition.partition(source, message.getContent(), 0);
  }

  public void send(int source, IMessage message, int dest, int flags) {
    throw new RuntimeException("Send with dest in PartitionStreamingOps is Not Implemented.");
  }

  /*
  * TODO:  The PartitionReceiver needs the partionOperator. Check a clear way to expose that from
  * TODO:  ops.
  *
  * **/
  public class PartitionReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      if ((flags & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
        if (!checkpointStarted) {
          checkpointStarted = true;
        }
        barrierMap.putIfAbsent(source, true);
        if (barrierMap.keySet() == partition.getOperation().getSources()) {
          partition.getOperation().getDestinations();
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
      return true;
    }

    @Override
    public void onFinish(int source) {

    }

    @Override
    public boolean progress() {
      return true;
    }
  }

  public boolean progress() {
    return partition.progress();
  }
}
