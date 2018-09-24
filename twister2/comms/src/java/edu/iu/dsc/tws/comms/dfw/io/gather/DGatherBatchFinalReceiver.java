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
package edu.iu.dsc.tws.comms.dfw.io.gather;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowGather;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.shuffle.FSMerger;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class DGatherBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(
      DGatherBatchFinalReceiver.class.getName());

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  /**
   * Finished sources for each target
   */
  private Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  /**
   * Final messages
   */
  private Map<Integer, List<Object>> finalMessages = new HashMap<>();
  /**
   * Send pending max
   */
  private int sendPendingMax = 128;
  /**
   * The receiver
   */
  private BulkReceiver bulkReceiver;
  /**
   * Weather this target is done
   */
  private Map<Integer, Boolean> batchDone = new HashMap<>();
  /**
   * Sort mergers for each target
   */
  private Map<Integer, FSMerger> sortedMergers = new HashMap<>();
  /**
   * Shuffler directory
   */
  private String shuffleDirectory;
  /**
   * The actual operation
   */
  private DataFlowGather gather;

  private KryoSerializer kryoSerializer;

  public DGatherBatchFinalReceiver(BulkReceiver bulkReceiver, String shuffleDir) {
    this.bulkReceiver = bulkReceiver;
    this.shuffleDirectory = shuffleDir;
    this.kryoSerializer = new KryoSerializer();
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    int maxBytesInMemory = DataFlowContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = DataFlowContext.getShuffleMaxRecordsInMemory(cfg);

    gather = (DataFlowGather) op;
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
        finishedPerTask.put(i, false);
      }
      messages.put(e.getKey(), messagesPerTask);
      finished.put(e.getKey(), finishedPerTask);
      finalMessages.put(e.getKey(), new ArrayList<>());
      batchDone.put(e.getKey(), false);

      FSMerger merger = new FSMerger(maxBytesInMemory, maxRecordsInMemory, shuffleDirectory,
          getOperationName(e.getKey()), gather.getDataType());
      sortedMergers.put(e.getKey(), merger);
    }
    this.bulkReceiver.init(cfg, expectedIds.keySet());
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;
    Queue<Object> m = messages.get(target).get(source);
    Map<Integer, Boolean> finishedMessages = finished.get(target);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      finishedMessages.put(source, true);
      return true;
    }
    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      m.add(object);
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }

  /**
   * Method used to communicationProgress work
   */
  @SuppressWarnings("unchecked")
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int t : messages.keySet()) {
      FSMerger fsMerger = sortedMergers.get(t);
      if (batchDone.get(t)) {
        continue;
      }

      boolean allFinished = true;
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> map = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);

      boolean found = true;
      boolean moreThanOne = false;
      for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
        if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
          found = false;
        } else {
          moreThanOne = true;
        }

        if (!finishedForTarget.get(e.getKey())) {
          allFinished = false;
        }
      }

      // if we have queues with 0 and more than zero we need further progress
      if (!found && moreThanOne) {
        needsFurtherProgress = true;
      }

      if (found) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          Queue<Object> valueList = e.getValue();
          if (valueList.size() > 0) {
            Object value = valueList.poll();
            if (value instanceof List) {
              out.addAll((List) value);
            } else {
              out.add(value);
            }
            allFinished = false;
          }
        }
        for (Object o : out) {
          byte[] d = DataSerializer.serialize(o, kryoSerializer);
          fsMerger.add(d, d.length);
        }
      } else {
        allFinished = false;
      }

      if (allFinished) {
        batchDone.put(t, true);
        fsMerger.switchToReading();
        bulkReceiver.receive(t, fsMerger.readIterator());
        // we can call on finish at this point
        onFinish(t);
      }
    }
    return needsFurtherProgress;
  }

  private String getOperationName(int target) {
    int edge = gather.getEdge();
    return "gather-" + edge + "-" + target + "-" + UUID.randomUUID().toString();
  }
}
