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
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;

/**
 * Partial receiver is only going to get called for messages going to other destinations
 * We have partial receivers for each actual source
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
  private int source;

  /**
   * The lock for excluding onMessage and progress
   */
  private Lock lock = new ReentrantLock();

  /**
   * Weather the operation has finished
   */
  private boolean finish = false;

  /**
   * we have sent to these destinations
   */
  private Set<Integer> finishedDestinations = new HashSet<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    lowWaterMark = DataFlowContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = DataFlowContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);
    executor = op.getTaskPlan().getThisExecutor();

    destinations = ((DataFlowPartition) op).getDestinations();
    this.operation = op;

    // lists to keep track of messages for destinations
    for (int d : destinations) {
      destinationMessages.put(d, new ArrayList<>());
    }
  }

  @Override
  public boolean onMessage(int src, int destination, int target, int flags, Object object) {
    this.source = src;
    List<Object> dests = destinationMessages.get(destination);

    int size = dests.size();
    if (size > highWaterMark) {
      return false;
    }

    dests.add(object);

    if (dests.size() > lowWaterMark) {
      lock.lock();
      try {
        readyToSend.put(destination, new ArrayList<>(dests));
        dests.clear();
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  @Override
  public void progress() {
    lock.lock();
    try {
      if (finish && readyToSend.isEmpty() && finishedDestinations.size() != destinations.size()) {
        for (int dest : destinations) {
          if (!finishedDestinations.contains(dest)) {
            if (operation.sendPartial(source, new byte[1], MessageFlags.EMPTY, dest)) {
              finishedDestinations.add(dest);
            } else {
              // no point in going further
              break;
            }
          }
        }
        return;
      }

      Iterator<Map.Entry<Integer, List<Object>>> it = readyToSend.entrySet().iterator();

      while (it.hasNext()) {
        Map.Entry<Integer, List<Object>> e = it.next();
        List<Object> send = new ArrayList<>(e.getValue());

        // if we send this list successfully
        if (operation.sendPartial(source, send, 0, e.getKey())) {
          // lets remove from ready list and clear the list
          e.getValue().clear();
          it.remove();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onFinish(int target) {
    // flush everything
    lock.lock();
    try {
      for (Map.Entry<Integer, List<Object>> e : destinationMessages.entrySet()) {
        List<Object> messages = new ArrayList<>();
        Integer key = e.getKey();
        if (readyToSend.containsKey(key)) {
          messages = readyToSend.get(key);
        } else {
          readyToSend.put(key, messages);
        }
        messages.addAll(e.getValue());
      }
      // finished
      finish = true;
    } finally {
      lock.unlock();
    }
  }
}
