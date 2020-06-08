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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.ArrayUtils;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.table.ArrowColumn;
import edu.iu.dsc.tws.common.table.ArrowTableBuilder;
import edu.iu.dsc.tws.common.table.OneRow;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.TableBuilder;
import edu.iu.dsc.tws.comms.table.ArrowAllToAll;
import edu.iu.dsc.tws.comms.table.ArrowCallback;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class TPartition extends BaseOperation {
  private ArrowAllToAll allToAll;

  private DestinationSelector selector;
  private int[] indexes;
  private Map<Integer, TableBuilder> partitionedTables = new HashMap<>();
  private Map<Integer, Queue<Table>> inputs = new HashMap<>();
  private boolean finished = false;
  private Set<Integer> finishedSources = new HashSet<>();
  private Set<Integer> thisWorkerSources;
  private Schema schema;
  private RootAllocator allocator;

  /**
   * Create the base operation
   */
  public TPartition(Communicator comm, IWorkerController controller, LogicalPlanBuilder builder,
                    DestinationSelector selector, List<Integer> indexes,
                    Schema schema, ArrowCallback receiver, RootAllocator allocator) {
    this(comm, controller, builder.getSources(), builder.getTargets(), builder.build(),
        selector, indexes, schema, receiver, allocator);
  }
  /**
   * Create the base operation
   */
  public TPartition(Communicator comm, IWorkerController controller, Set<Integer> sources,
                    Set<Integer> targets, LogicalPlan plan,
                    DestinationSelector selector, List<Integer> indexes,
                    Schema schema, ArrowCallback receiver, RootAllocator allocator) {
    super(comm, false, CommunicationContext.TABLE_PARTITION);
    this.selector = selector;
    if (indexes == null) {
      this.indexes = new int[]{0};
    } else {
      this.indexes = ArrayUtils.toPrimitive(indexes.toArray(new Integer[0]));
    }

    this.schema = schema;
    this.allocator = allocator;
    for (int s : sources) {
      this.inputs.put(s, new LinkedList<>());
    }
    this.thisWorkerSources = TaskPlanUtils.getTasksOfThisWorker(plan, sources);

    this.allToAll = new ArrowAllToAll(comm.getConfig(), controller, sources, targets,
        plan, comm.nextEdge(), receiver, schema, allocator);
  }

  public boolean insert(int source, Table t) {
    // partition the table, default
    return inputs.get(source).offer(t);
  }

  @Override
  public boolean isComplete() {
    for (Map.Entry<Integer, Queue<Table>> e : inputs.entrySet()) {
      if (e.getValue().isEmpty()) {
        continue;
      }
      // partition the table, default
      Table t = e.getValue().poll();

      List<ArrowColumn> columns = t.getColumns();
      ArrowColumn col = columns.get(indexes[0]);
      for (int i = 0; i < col.getVector().getValueCount(); i++) {
        Row row = new OneRow(col.get(i));
        int target = selector.next(e.getKey(), row);

        TableBuilder builder = partitionedTables.get(target);
        if (builder == null) {
          builder = new ArrowTableBuilder(schema, allocator);
          this.partitionedTables.put(target, builder);
        }
        for (int j = 0; j < columns.size(); j++) {
          builder.getColumns().get(j).addValue(columns.get(j).get(i));
        }
      }
    }

    if (finished) {
      for (Map.Entry<Integer, TableBuilder> e : partitionedTables.entrySet()) {
        Table t = e.getValue().build();
        allToAll.insert(t, e.getKey());
      }

      // clear the tables, so we won't build the tables again
      partitionedTables.clear();
      for (int s : finishedSources) {
        allToAll.finish(s);
      }
      // clear so, we won't call finish again
      finishedSources.clear();
      return allToAll.isComplete();
    }
    return false;
  }

  @Override
  public void finish(int src) {
    finishedSources.add(src);
    if (finishedSources.size() == thisWorkerSources.size()) {
      finished = true;
    }
  }

  @Override
  public boolean progress() {
    return !finished || !allToAll.isComplete();
  }

  @Override
  public void close() {
    allToAll.close();
  }

  @Override
  public void reset() {
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
    while (!isComplete()) {
      progress();
    }
  }
}
