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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class GatherOperation extends AbstractParallelOperation {

  private MPIDataFlowGather op;

  public GatherOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e,
                      DataType dataType, String edgeName, Config config, TaskPlan taskPlan) {
    this.edge = e;
    op = new MPIDataFlowGather(channel, srcs, dest, new GatherReceiver(), 0, 0, config,
        MessageType.INTEGER, taskPlan, e.getIntegerMapping(edgeName));

    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public void send(int source, IMessage message) {
    super.send(source, message);
  }

  @Override
  public void send(int source, IMessage message, int dest) {
    op.send(source, message, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }

  public class GatherReceiver implements MessageReceiver {

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      TaskMessage msg = new TaskMessage(object,
          edge.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public void progress() {

    }
  }
}
