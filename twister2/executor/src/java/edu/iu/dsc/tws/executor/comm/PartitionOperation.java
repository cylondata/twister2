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
package edu.iu.dsc.tws.executor.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.barrier.CheckpointBarrier;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionOperation.class.getName());
  private HashMap<Integer, ArrayList<Integer>> barrierMap = new HashMap<>();
  private HashMap<Integer, Integer> incommingMap = new HashMap<>();
  private HashMap<Integer, ArrayList<Object>> incommingBuffer = new HashMap<>();

  protected DataFlowPartition op;

  public PartitionOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    //LOG.info("ParitionOperation Prepare 1");
    op = new DataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        new PartitionPartialReceiver(), DataFlowPartition.PartitionStratergy.DIRECT);
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, DataType keyType, String edgeName) {
    this.edge = e;
    op = new DataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        new PartitionPartialReceiver(), DataFlowPartition.PartitionStratergy.DIRECT,
        Utils.dataTypeToMessageType(dataType), Utils.dataTypeToMessageType(keyType));
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  public void send(int source, IMessage message) {
    op.send(source, message.getContent(), 0);
  }

  public void send(int source, IMessage message, int dest) {
    op.send(source, message, 0, dest);
  }

  public class PartitionReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      if (barrierMap.containsKey(source)) {
        if (barrierMap.get(source).size() == 0) {
          barrierMap.remove(source);
          if (object instanceof List) {
            for (Object o : (List) object) {
              TaskMessage msg = new TaskMessage(o,
                  edge.getStringMapping(communicationEdge), target);
              outMessages.get(target).offer(msg);
            }
          } else if (object instanceof CheckpointBarrier) {
            barrierMap.get(source).add(destination);
            ArrayList<Object> bufferMessege = new ArrayList<>();
            bufferMessege.add(object);
            incommingBuffer.put(source, bufferMessege);
          }
        } else if (barrierMap.get(source).size() == incommingMap.get(source)) {
          for (Object message : incommingBuffer.get(source)) {
            if (message instanceof List) {
              for (Object o : (List) message) {
                TaskMessage msg = new TaskMessage(o,
                    edge.getStringMapping(communicationEdge), target);
                outMessages.get(target).offer(msg);
              }
            }
          }
        }
      } else {
        if (object instanceof List) {
          for (Object o : (List) object) {
            TaskMessage msg = new TaskMessage(o,
                edge.getStringMapping(communicationEdge), target);
            outMessages.get(target).offer(msg);

          }
        } else if (object instanceof CheckpointBarrier) {
          ArrayList<Integer> destinationMap = new ArrayList<Integer>();
          destinationMap.add(destination);
          barrierMap.put(source, destinationMap);
        }

      }
      return true;
    }

    @Override
    public void onFinish(int target) {

    }

    @Override
    public void progress() {
    }
  }

  public void progress() {
    op.progress();
  }
}
