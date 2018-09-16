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
package edu.iu.dsc.tws.comms.dfw.io.reduce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class ReduceBatchReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchReceiver.class.getName());

  protected ReduceFunction reduceFunction;

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  protected Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  protected Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  protected Map<Integer, Set<Integer>> emptyReceivedSources = new HashMap<>();
  protected DataFlowOperation dataFlowOperation;
  protected int executor;
  protected int sendPendingMax = 128;
  protected int destination;
  protected Map<Integer, Boolean> batchDone = new HashMap<>();
  protected Map<Integer, Boolean> isEmptySent = new HashMap<>();
  protected Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();

  //Variables related to message buffering
  protected int bufferSize = 10;
  protected boolean bufferTillEnd = false;
  protected Map<Integer, Integer> bufferCounts = new HashMap<>();
  protected Map<Integer, Object> reducedValueMap = new HashMap<>();


  public ReduceBatchReceiver(ReduceFunction reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  public ReduceBatchReceiver(int dst, ReduceFunction reduce) {
    this.destination = dst;
    this.reduceFunction = reduce;
  }

  public ReduceBatchReceiver(int dst, ReduceFunction reduce, int buffSize, boolean buffTillEnd) {
    this.destination = dst;
    this.reduceFunction = reduce;
    this.bufferSize = buffSize;
    this.bufferTillEnd = buffTillEnd;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();

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
      emptyReceivedSources.put(e.getKey(), new HashSet<>());
      counts.put(e.getKey(), countsPerTask);
      batchDone.put(e.getKey(), false);
      isEmptySent.put(e.getKey(), false);
      totalCounts.put(e.getKey(), totalCountsPerTask);

      bufferCounts.put(e.getKey(), 0);
      reducedValueMap.put(e.getKey(), null);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d", executor, target));
    }

    Map<Integer, Boolean> finishedMessages = finished.get(target);

    if ((flags & MessageFlags.END) == MessageFlags.END) {
      finishedMessages.put(source, true);
      return true;
    }

    Queue<Object> m = messages.get(target).get(source);

    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }
      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      Integer tc = totalCounts.get(target).get(source);
      totalCounts.get(target).put(source, tc + 1);
      //LOG.info("Flags : " + flags + ", Message Flag Last : " + MessageFlags.FLAGS_LAST);
      m.add(object);
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finishedMessages.put(source, true);
        //LOG.info("onMessage ReduceBatchReceiver Final Message Added");
      }
    }
    return canAdd;
  }

  @Override
  public void onFinish(int source) {
    for (Integer target : finished.keySet()) {
      Map<Integer, Boolean> finishedMessages = finished.get(target);
      finishedMessages.put(source, true);
    }
  }
}
