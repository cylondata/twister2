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
package edu.iu.dsc.tws.executor.comm.operations.batch;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BGather;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;


public class GatherBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(GatherBatchOperation.class.getName());
  protected BGather gather;
  private Communicator communicator;
  private TaskPlan tPlan;

  public GatherBatchOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
    this.communicator = new Communicator(config, network);
    this.tPlan = tPlan;
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e,
                      MessageType dataType, String edgeName, Config config, TaskPlan taskPlan) {
    gather = new BGather(communicator, taskPlan, srcs, dest, dataType, new FinalGatherReceiver());
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e, MessageType dataType,
                      MessageType keyType, String edgeName, Config config, TaskPlan taskPlan) {
    gather = new BGather(communicator, taskPlan, srcs, dest, dataType, new FinalGatherReceiver());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.info("Message : " + message.getContent());
    return gather.gather(source, message.getContent(), flags);
  }

  @Override
  public void send(int source, IMessage message, int dest, int flags) {
    //LOG.info("Message : " + message.getContent());
    throw new RuntimeException("send with dest not Implemented in GatherBatchOperation");
  }

  @Override
  public boolean progress() {
    return gather.progress();
  }

  public boolean hasPending() {
    return gather.hasPending();
  }


  private class FinalGatherReceiver implements BatchReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public void receive(int target, Iterator<Object> it) {
      if (it instanceof List) {
        for (Object o : (List) it) {
          TaskMessage msg = new TaskMessage(o,
              edge.getStringMapping(communicationEdge), target);
          outMessages.get(target).offer(msg);
          //    LOG.info("Source : " + source + ", Message : " + msg.getContent() + ", Target : "
          //        + target + ", Destination : " + destination);

        }
      }
    }
  }

}
