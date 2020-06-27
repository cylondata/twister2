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
package edu.iu.dsc.tws.comms.table.ops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.comms.table.ArrowCallback;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class BTPartition extends BaseOperation {
  private STPartition partition;
  private Map<Integer, List<Object>> receivedTables = new HashMap<>();
  private boolean completed = false;
  private BulkReceiver receiver;

  public BTPartition(Communicator comm, IWorkerController controller, Set<Integer> sources,
                     Set<Integer> targets, LogicalPlan plan, DestinationSelector selector,
                     List<Integer> indexes, Schema schema,
                     BulkReceiver receiver, RootAllocator allocator, String name) {
    super(comm, false, name);
    this.receiver = receiver;
    partition = new STPartition(comm, controller, sources, targets, plan, selector, indexes, schema,
        new ArrowReceiverToBulk(), allocator, name);
    Set<Integer> thisTargets = TaskPlanUtils.getTasksOfThisWorker(plan, targets);
    for (int t : thisTargets) {
      receivedTables.put(t, new ArrayList<>());
    }
  }

  public boolean insert(int source, Table t) {
    // partition the table, default
    return partition.insert(source, t);
  }

  @Override
  public boolean isComplete() {
    boolean complete = partition.isComplete();
    if (complete && !completed) {
      for (Map.Entry<Integer, List<Object>> e : receivedTables.entrySet()) {
        receiver.receive(e.getKey(), e.getValue().iterator());
        receiver.sync(e.getKey(), new byte[0]);
      }
      completed = true;
    }
    return complete;
  }

  @Override
  public void finish(int src) {
    partition.finish(src);
  }

  @Override
  public boolean progress() {
    return partition.progress();
  }

  @Override
  public void close() {
    partition.close();
  }

  @Override
  public void reset() {
    partition.reset();
  }

  @Override
  public boolean progressChannel() {
    return progress();
  }

  @Override
  public boolean sendBarrier(int src, byte[] barrierId) {
    throw new Twister2RuntimeException("Not-implemented");
  }

  @Override
  public void waitForCompletion() {
    partition.waitForCompletion();
  }

  private class ArrowReceiverToBulk implements ArrowCallback {
    @Override
    public void onReceive(int source, int target, Table table) {
      List<Object> recvTables = receivedTables.get(target);
      recvTables.add(table);
    }
  }
}
