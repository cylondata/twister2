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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
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
import edu.iu.dsc.tws.comms.table.ops.BTPartition;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class PartitionOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionOperation.class.getName());

  protected BTPartition op;
  private boolean syncCalled = false;
  private Set<Integer> thisTargets;

  public PartitionOperation(Config config, Communicator network, LogicalPlan tPlan,
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

    thisTargets = TaskPlanUtils.getTasksOfThisWorker(tPlan, targets);

    TableRuntime runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    assert runtime != null;

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    Schema schema = (Schema) edge.getProperty(CommunicationContext.ROW_SCHEMA);
    op = new BTPartition(newComm, WorkerEnvironment.getWorkerEnv().getWorkerController(),
        sources, targets, tPlan, destSelector, indexes, schema,
        new PartitionReceiver(), runtime.getRootAllocator(), CommunicationContext.TABLE_PARTITION);
  }

  public synchronized boolean send(int source, IMessage message, int dest) {
    return op.insert(source, (Table) message.getContent());
  }

  public class PartitionReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> targets) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      LOG.info(String.format("Received table to %d -> %d", target, target));
      TaskMessage msg = new TaskMessage<>(it, inEdge, target);
      outMessages.get(target).offer(msg);
      return true;
    }
  }

  @Override
  public synchronized boolean isComplete() {
    // first progress
    this.progress();

    // then check isComplete
    boolean complete = this.getOp().isComplete();
    if (complete && !syncCalled) {
      for (int target : thisTargets) {
        syncs.get(target).sync(inEdge, null);
      }
      syncCalled = true;
    }
    return complete;
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
