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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class ReduceStreamingReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceStreamingReceiver.class.getName());

  protected ReduceFunction reduceFunction;
  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  protected Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  protected int executor;
  protected int count = 0;
  protected DataFlowOperation operation;
  protected int sendPendingMax = 128;
  protected int destination;
  private Map<Integer, Queue<Object>> reducedValuesMap = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();

  public ReduceStreamingReceiver(ReduceFunction function) {
    this(0, function);
  }

  public ReduceStreamingReceiver(int dst, ReduceFunction function) {
    this.reduceFunction = function;
    this.destination = dst;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    this.executor = op.getTaskPlan().getThisExecutor();
    this.operation = op;
    this.sendPendingMax = DataFlowContext.sendPendingMax(cfg);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();
      Map<Integer, Integer> totalCountsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
        countsPerTask.put(i, 0);
        totalCountsPerTask.put(i, 0);
      }

      LOG.fine(String.format("%d Final Task %d receives from %s",
          executor, e.getKey(), e.getValue().toString()));

      reducedValuesMap.put(e.getKey(), new ArrayBlockingQueue<>(sendPendingMax));
      messages.put(e.getKey(), messagesPerTask);
      counts.put(e.getKey(), countsPerTask);
      totalCounts.put(e.getKey(), totalCountsPerTask);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;
    Queue<Object> m = messages.get(target).get(source);
    Integer c = counts.get(target).get(source);
    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      m.offer(object);
      counts.get(target).put(source, c + 1);

      Integer tc = totalCounts.get(target).get(source);
      totalCounts.get(target).put(source, tc + 1);
    }

    return canAdd;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int t : messages.keySet()) {
      boolean canProgress = true;
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(t);
      Map<Integer, Integer> countsPerTarget = counts.get(t);
      Queue<Object> reducedValues = this.reducedValuesMap.get(t);

      while (canProgress) {
        boolean found = true;
        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
            canProgress = false;
          } else {
            moreThanOne = true;
          }
        }
        // if we have queues with 0 and more than zero we need further communicationProgress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;
        }

        if (found && reducedValues.size() < sendPendingMax) {
          Object previous = null;
          for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
            if (previous == null) {
              previous = e.getValue().poll();
            } else {
              Object current = e.getValue().poll();
              previous = reduceFunction.reduce(previous, current);
            }
          }
          if (previous != null) {
            reducedValues.offer(previous);
          }
        }

        if (reducedValues.size() > 0) {
          Object previous = reducedValues.peek();
          boolean handle = handleMessage(t, previous, 0, destination);
          if (handle) {
            reducedValues.poll();
            for (Map.Entry<Integer, Integer> e : countsPerTarget.entrySet()) {
              Integer i = e.getValue();
              countsPerTarget.put(e.getKey(), i - 1);
            }
          } else {
            canProgress = false;
            needsFurtherProgress = true;
          }
        }
      }
    }
    return needsFurtherProgress;
  }

  public abstract boolean handleMessage(int source, Object message, int flags, int dest);
}
