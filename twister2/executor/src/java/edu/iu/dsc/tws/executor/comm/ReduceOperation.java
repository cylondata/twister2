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
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;

public class ReduceOperation extends AbstractParallelOperation {

  private static final Logger LOG = Logger.getLogger(ReduceOperation.class.getName());

  protected MPIDataFlowReduce op;

  public ReduceOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> sources, int dest, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    op = new MPIDataFlowReduce(channel, sources, dest, new ReduceReceiver(),
        new ReducePartialReceiver());
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public void send(int source, IMessage message) {
    op.send(source, message.getContent(), 0);
  }

  @Override
  public void send(int source, IMessage message, int dest) {
    op.send(source, message, 0, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }

  public class ReduceReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
//      LOG.info("Reduce Final Receiver : " + source + ", " + destination + ", Object : "
//          + (String) object);


      return true;
    }

    @Override
    public void progress() {
    }
  }

  public class ReducePartialReceiver implements MessageReceiver {

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
//      LOG.info("Reduce Partial Receiver : " + source + ", " + destination + ", Object : "
//          + (String) object);

      return true;
    }

    @Override
    public void progress() {

    }
  }

}
