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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.comms.table.ArrowCallback;
import edu.iu.dsc.tws.comms.table.ops.TPartition;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;

public class DirectOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(PipeOperation.class.getName());

  protected TPartition op;
  private boolean syncCalled = false;
  private Set<Integer> thisTargets;

  public DirectOperation(Config config, Communicator network, LogicalPlan tPlan,
                       Set<Integer> sources, Set<Integer> targets, Edge edge,
                       Map<Integer, Integer> srcGlobalToIndex,
                       Map<Integer, Integer> tgtsGlobalToIndex) {
    super(config, network, tPlan, edge.getName());

    List<Integer> indexes = null;
    Object indexesProp = edge.getProperty("indexes");
    if (indexesProp instanceof List) {
      indexes = (List<Integer>) indexesProp;
    }

    ArrayList<Integer> sList = new ArrayList<>(sources);
    Collections.sort(sList);
    ArrayList<Integer> targList = new ArrayList<>(targets);
    Collections.sort(targList);
    Map<Integer, Integer> sourceToTarget = new HashMap<>();
    for (int i = 0; i < sList.size(); i++) {
      sourceToTarget.put(sList.get(i), targList.get(i));
    }
    DestinationSelector destSelector = new OneToOneDestSelector(sourceToTarget);
    thisTargets = TaskPlanUtils.getTasksOfThisWorker(tPlan, targets);

    TableRuntime runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    assert runtime != null;

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    Schema schema = (Schema) edge.getProperty(CommunicationContext.ROW_SCHEMA);
    op = new TPartition(newComm, WorkerEnvironment.getWorkerEnv().getWorkerController(),
        sources, targets, tPlan, destSelector, indexes, schema,
        new PartitionReceiver(), runtime.getRootAllocator(), CommunicationContext.TABLE_PIPE);
  }

  private class OneToOneDestSelector implements DestinationSelector {
    private Map<Integer, Integer> srcToTgtMapper = new HashMap<>();

    OneToOneDestSelector(Map<Integer, Integer> srcToTgtMapper) {
      this.srcToTgtMapper = srcToTgtMapper;
    }

    @Override
    public int next(int source, Object data) {
      return srcToTgtMapper.get(source);
    }
  }

  public synchronized boolean send(int source, IMessage message, int dest) {
    return op.insert(source, (Table) message.getContent());
  }

  public class PartitionReceiver implements ArrowCallback {
    @Override
    public void onReceive(int source, int target, Table table) {
      LOG.info(String.format("Received table to %d -> %d", source, target));
      TaskMessage msg = new TaskMessage<>(table, inEdge, source);
      outMessages.get(target).offer(msg);
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
