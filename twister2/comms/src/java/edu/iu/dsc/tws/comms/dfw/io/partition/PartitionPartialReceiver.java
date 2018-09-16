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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

/**
 * This is the partial receiver for the partition operation.
 * Partial receiver is only going to get called for messages going to other destinations
 * We have partial receivers for each actual source, So even if the message is going to be forwarded
 * to a task within the same worker the message will still go through the partial receiver.
 */
public class PartitionPartialReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionPartialReceiver.class.getName());

  /**
   * Low water mark
   */
  private int lowWaterMark = 8;

  /**
   * High water mark to keep track of objects
   */
  private int highWaterMark = 16;

  /**
   * The executor
   */
  protected int executor;

  /**
   * The destinations set
   */
  private Set<Integer> destinations;

  /**
   * Keep the destination messages
   */
  private Map<Integer, List<Object>> destinationMessages = new HashMap<>();

  /**
   * Keep the list of tuple [Object, Source, Flags] for each destination
   */
  private Map<Integer, List<Object>> readyToSend = new HashMap<>();

  /**
   * The dataflow operation
   */
  private DataFlowOperation operation;

  /**
   * The source task connected to this partial receiver
   */
  private int representSource;

  /**
   * Tracks if the representSource is already set
   */
  private boolean representSourceIsSet;

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  private Lock lock = new ReentrantLock();

  /**
   * we have sent to these destinations
   */
  private Map<Integer, Set<Integer>> finishedDestinations = new HashMap<>();

  /**
   * These sources called onFinished
   */
  private Set<Integer> onFinishedSources = new HashSet<>();

  /**
   * Sources of this worker
   */
  private Set<Integer> thisWorkerSources = new HashSet<>();

  /**
   * Progress attempts without sending
   */
  private int progressAttempts = 0;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    lowWaterMark = DataFlowContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = DataFlowContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);
    executor = op.getTaskPlan().getThisExecutor();
    TaskPlan taskPlan = op.getTaskPlan();
    thisWorkerSources = TaskPlanUtils.getTasksOfThisWorker(taskPlan,
        ((DataFlowPartition) op).getSources());
    for (int s : thisWorkerSources) {
      finishedDestinations.put(s, new HashSet<>());
    }

    destinations = ((DataFlowPartition) op).getDestinations();
    this.operation = op;

    // lists to keep track of messages for destinations
    for (int d : destinations) {
      destinationMessages.put(d, new ArrayList<>());
    }
  }

  /**
   * All message that come to the partial receiver are handled by this method. Since we currently
   * do not have a need to know the exact source at the receiving end for the parition operation
   * this method uses a representative source that is used when forwarding the message to its true
   * target
   *
   * @param src the source of the message
   * @param path the path that is taken by the message, that is intermediate targets
   * @param target the target of this receiver
   * @param flags the communication flags
   * @param object the actual message
   * @return true if the message was successfully forwarded or queued.
   */
  @Override
  public boolean onMessage(int src, int path, int target, int flags, Object object) {
    lock.lock();
    try {
      if (!representSourceIsSet) {
        this.representSource = src;
      }

      List<Object> dests = destinationMessages.get(target);

      int size = dests.size();
      if (size > highWaterMark) {
        return false;
      }

      if ((flags & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
        dests.add(object);
        if (readyToSend.isEmpty()) {
          operation.sendPartial(representSource, new ArrayList<>(dests), 0, target);
        } else {
          Iterator<Map.Entry<Integer, List<Object>>> it = readyToSend.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<Integer, List<Object>> e = it.next();
            List<Object> send = new ArrayList<>(e.getValue());

            // if we send this list successfully
            if (operation.sendPartial(representSource, send, 0, e.getKey())) {
              // lets remove from ready list and clear the list
              e.getValue().clear();
              it.remove();
            }
          }
          operation.sendPartial(representSource, new ArrayList<>(dests), 0, target);
        }
      } else {
        dests.add(object);
        if (dests.size() > lowWaterMark) {
          swapToReady(target, dests);
        }

      }
    } finally {
      lock.unlock();
    }
    return true;
  }

  private void swapToReady(int destination, List<Object> dests) {
    if (!readyToSend.containsKey(destination)) {
      readyToSend.put(destination, new ArrayList<>(dests));
    } else {
      List<Object> ready = readyToSend.get(destination);
      ready.addAll(dests);
    }
    dests.clear();
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    lock.lock();

    if (progressAttempts > 2) {
      for (Map.Entry<Integer, List<Object>> e : destinationMessages.entrySet()) {
        if (e.getValue().size() > 0) {
          swapToReady(e.getKey(), e.getValue());
        }
      }
      progressAttempts = 0;
    }

    try {

      Iterator<Map.Entry<Integer, List<Object>>> it = readyToSend.entrySet().iterator();

      while (it.hasNext()) {

        Map.Entry<Integer, List<Object>> e = it.next();
        List<Object> send = new ArrayList<>(e.getValue());
        if (send.size() == 0) {
          e.getValue().clear();
          it.remove();
          progressAttempts = 0;
          continue;
        }
        // if we send this list successfully
        if (operation.sendPartial(representSource, send, 0, e.getKey())) {
          // lets remove from ready list and clear the list
          e.getValue().clear();
          it.remove();
          progressAttempts = 0;
        } else {
          needsFurtherProgress = true;
        }
      }

      for (Map.Entry<Integer, List<Object>> e : destinationMessages.entrySet()) {
        if (e.getValue().size() > 0) {
          needsFurtherProgress = true;
        }
      }

      for (Map.Entry<Integer, List<Object>> e : readyToSend.entrySet()) {
        if (e.getValue().size() > 0) {
          needsFurtherProgress = true;
        }
      }
      if (operation.isDelegeteComplete() && !needsFurtherProgress
          && onFinishedSources.equals(thisWorkerSources)
          && readyToSend.isEmpty()) {
        for (int source : thisWorkerSources) {
          Set<Integer> finishedDestPerSource = finishedDestinations.get(source);
          for (int dest : destinations) {
            if (!finishedDestPerSource.contains(dest)) {
              if (operation.sendPartial(source, new byte[1], MessageFlags.END, dest)) {
                finishedDestPerSource.add(dest);
                progressAttempts = 0;
              } else {
                needsFurtherProgress = true;
                // no point in going further
                break;
              }
            }
          }
        }
        return needsFurtherProgress;
      }
    } finally {
      lock.unlock();
    }

    progressAttempts++;
    return needsFurtherProgress;
  }

  /**
   * Is called once the source tasks complete sending message to the partition operation,
   * this does not mean that all the messages related to the given source is processed and sent to
   * their final targets, this just means that the source task will not be sending any message after
   * this method has been called, the message sent previously may still be queued in the system and
   * not reached the partial receiver yet.
   *
   * @param source the source for which message sending has completed
   */
  @Override
  public void onFinish(int source) {
    // flush everything
    lock.lock();
    try {
      for (Map.Entry<Integer, List<Object>> e : destinationMessages.entrySet()) {
        swapToReady(e.getKey(), e.getValue());
      }
      // finished
      onFinishedSources.add(source);
    } finally {
      lock.unlock();
    }
  }
}
