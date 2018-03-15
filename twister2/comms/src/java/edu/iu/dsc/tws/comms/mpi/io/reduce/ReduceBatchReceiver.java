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
package edu.iu.dsc.tws.comms.mpi.io.reduce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;

public abstract class ReduceBatchReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchReceiver.class.getName());

  protected ReduceFunction reduceFunction;

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  protected Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  protected Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  protected DataFlowOperation dataFlowOperation;
  protected int executor;
  protected int sendPendingMax = 128;
  protected int destination;
  protected Map<Integer, Boolean> batchDone = new HashMap<>();
  protected Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();
  protected Queue<Object> reducedValues;

  public ReduceBatchReceiver(ReduceFunction reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  public ReduceBatchReceiver(int dst, ReduceFunction reduce) {
    this.destination = dst;
    this.reduceFunction = reduce;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = MPIContext.sendPendingMax(cfg);
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();
    this.reducedValues = new ArrayBlockingQueue<>(sendPendingMax);

    LOG.fine(String.format("%d gather partial expected ids %s", executor, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();
      Map<Integer, Integer> totalCountsPerTask = new HashMap<>();

      for (int task : e.getValue()) {
        messagesPerTask.put(task, new ArrayBlockingQueue<>(sendPendingMax));
        finishedPerTask.put(task, false);
        countsPerTask.put(task, 0);
        totalCountsPerTask.put(task, 0);
      }
      messages.put(e.getKey(), messagesPerTask);
      finished.put(e.getKey(), finishedPerTask);
      counts.put(e.getKey(), countsPerTask);
      batchDone.put(e.getKey(), false);
      totalCounts.put(e.getKey(), totalCountsPerTask);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d", executor, target));
    }
    Queue<Object> m = messages.get(target).get(source);
    Map<Integer, Boolean> finishedMessages = finished.get(target);

    if (m.size() >= sendPendingMax) {
      canAdd = false;
//      LOG.info(String.format("%d Partial add FALSE target %d source %d %s %s",
//          executor, target, source, finishedMessages, counts.get(target)));
    } else {
//      LOG.info(String.format("%d Partial add TRUE target %d source %d %s %s",
//          executor, target, source, finishedMessages, counts.get(target)));
      if (object instanceof MPIMessage) {
        ((MPIMessage) object).incrementRefCount();
      }
      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      Integer tc = totalCounts.get(target).get(source);
      totalCounts.get(target).put(source, tc + 1);

      m.add(object);
      if ((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) {
//        LOG.info(String.format("%d Received LAST FLAG", executor));
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }
}
