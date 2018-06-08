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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionOperation.class.getName());

  protected MPIDataFlowPartition op;

  public PartitionOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    LOG.info("ParitionOperation Prepare 1");
    op = new MPIDataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        new PartialPartitionReciver(), MPIDataFlowPartition.PartitionStratergy.DIRECT);
    partitionEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, partitionEdge);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, DataType keyType, String edgeName) {
    this.edge = e;
    op = new MPIDataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        new PartialPartitionReciver(), MPIDataFlowPartition.PartitionStratergy.DIRECT,
        Utils.dataTypeToMessageType(dataType), Utils.dataTypeToMessageType(keyType));
    partitionEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, partitionEdge);
  }

  public void send(int source, IMessage message) {
    op.send(source, message.getContent(), 0);
  }

  public void send(int source, IMessage message, int dest) {
    op.send(source, message, 0, dest);
  }

  private class PartialPartitionReciver implements MessageReceiver {

    @Override
    public void init(Config cfg, DataFlowOperation dfop, Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      //LOG.info("Partial Receiver");
      return false;
    }

    @Override
    public void progress() {

    }
  }

  public class PartitionReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
      LOG.info("PartitionReceiver Init");
    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      LOG.info("onMessage : Start");
      TaskMessage msg = new TaskMessage(object,
          edge.getStringMapping(partitionEdge), target);
      LOG.info("Source : " + source + ", Message : " + msg.getContent() + ", Target : "
          + target + ", Destination : " + destination);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public void progress() {
    }
  }

  public void progress() {
    op.progress();
  }
}
