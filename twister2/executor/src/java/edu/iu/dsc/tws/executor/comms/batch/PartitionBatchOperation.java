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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BPartition;
import edu.iu.dsc.tws.comms.op.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionBatchOperation.class.getName());
  private HashMap<Integer, ArrayList<Integer>> barrierMap = new HashMap<>();
  private HashMap<Integer, Integer> incommingMap = new HashMap<>();
  private HashMap<Integer, ArrayList<Object>> incommingBuffer = new HashMap<>();

  protected BPartition op;

  public PartitionBatchOperation(Config config, Communicator network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edgeGenerator = e;
    //LOG.info("ParitionOperation Prepare 1");
    op = new BPartition(channel, taskPlan, srcs, dests,
        Utils.dataTypeToMessageType(dataType), new PartitionReceiver(),
        new LoadBalanceSelector());
    communicationEdge = e.generate(edgeName);
  }

  public void send(int source, IMessage message) {
    op.partition(source, message.getContent(), 0);
  }

  public boolean send(int source, IMessage message, int dest) {
    return op.partition(source, message.getContent(), 0);
  }

  public class PartitionReceiver implements BatchReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      outMessages.get(target).offer(msg);
    }
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  @Override
  public boolean progress() {
    return op.progress() || hasPending();
  }

  public boolean hasPending() {
    return op.hasPending();
  }
}
