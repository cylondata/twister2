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
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowAllReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class AllReduceOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllReduceOperation.class.getName());

  protected DataFlowAllReduce op;

  public AllReduceOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> sources, Set<Integer>  dest, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    op = new DataFlowAllReduce(channel, sources, dest, 0, new IndentityFunction(),
        new FinalReduceReceive(), 0, 0, true);
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public void send(int source, IMessage message) {
    op.send(source, message.getContent(), 0);
  }

  @Override
  public void send(int source, IMessage message, int dest) {
    op.send(source, message.getContent(), 0, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }

  public static class IndentityFunction implements ReduceFunction {

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return t1;
    }
  }

  public class FinalReduceReceive implements ReduceReceiver {
    private int count = 0;

    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      TaskMessage msg = new TaskMessage(object,
          edge.getStringMapping(communicationEdge), target);
      outMessages.get(target).offer(msg);
      return true;
    }
  }
}
