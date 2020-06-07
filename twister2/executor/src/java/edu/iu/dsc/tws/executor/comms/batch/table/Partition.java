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
package edu.iu.dsc.tws.executor.comms.batch.table;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.table.ArrowCallback;
import edu.iu.dsc.tws.comms.table.ops.TPartition;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class Partition extends AbstractParallelOperation {

  protected TPartition op;

  public Partition(Config config, Communicator network, LogicalPlan tPlan,
                   Set<Integer> sources, Set<Integer> targets, Edge edge,
                   Map<Integer, Integer> srcGlobalToIndex,
                   Map<Integer, Integer> tgtsGlobalToIndex) {
    super(config, network, tPlan, edge.getName());
    MessageType dataType = edge.getDataType();
    String edgeName = edge.getName();
    boolean shuffle = false;

    Object shuffleProp = edge.getProperty("shuffle");
    if (shuffleProp instanceof Boolean && (Boolean) shuffleProp) {
      shuffle = true;
    }

    List<Integer> indexes = null;
    Object indexesProp = edge.getProperty("indexes");
    if (indexesProp instanceof List) {
      indexes = (List<Integer>) indexesProp;
    }

    DestinationSelector destSelector;
    if (edge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(edge.getPartitioner(),
          srcGlobalToIndex, tgtsGlobalToIndex);
    } else {
      destSelector = new HashingSelector();
    }

    TableRuntime runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    assert runtime != null;

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    Schema schema = (Schema) edge.getProperty(CommunicationContext.ROW_SCHEMA);
    //LOG.info("ParitionOperation Prepare 1");
    op = new TPartition(newComm, WorkerEnvironment.getWorkerEnv().getWorkerController(),
        sources, targets, tPlan, destSelector, indexes, schema,
        new PartitionReceiver(), runtime.getRootAllocator());
  }

  public boolean send(int source, IMessage message, int dest) {
    return op.insert(source, (Table) message.getContent());
  }

  public class PartitionReceiver implements ArrowCallback {
    @Override
    public void onReceive(int source, int target, Table table) {
      TaskMessage msg = new TaskMessage<>(table, inEdge, source);
      outMessages.get(target).offer(msg);
    }
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
