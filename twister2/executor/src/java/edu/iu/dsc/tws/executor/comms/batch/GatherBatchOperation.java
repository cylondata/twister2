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
package edu.iu.dsc.tws.executor.comms.batch;

import java.util.Iterator;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.batch.BGather;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;

public class GatherBatchOperation extends AbstractParallelOperation {

  private BGather op;

  public GatherBatchOperation(Config config, Communicator network, LogicalPlan tPlan,
                              Set<Integer> srcs, Set<Integer> dests, Edge edge) {
    super(config, network, tPlan, edge.getName());
    if (dests.size() > 1) {
      throw new RuntimeException("Gather can only have one target: " + dests);
    }
    Object shuffleProp = edge.getProperty("shuffle");
    boolean shuffle = false;
    if (shuffleProp instanceof Boolean && (Boolean) shuffleProp) {
      shuffle = true;
    }

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new BGather(newComm, logicalPlan, srcs, dests.iterator().next(),
        edge.getDataType(), new FinalGatherReceiver(), shuffle,
        edge.getEdgeID().nextId(), edge.getMessageSchema());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.info("Message : " + message.getContent());
    return op.gather(source, message.getContent(), flags);
  }

  private class FinalGatherReceiver implements BulkReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      // add the object to the map
      TaskMessage msg = new TaskMessage<>(it, inEdge, target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public boolean sync(int target, byte[] message) {
      return syncs.get(target).sync(inEdge, message);
    }
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
