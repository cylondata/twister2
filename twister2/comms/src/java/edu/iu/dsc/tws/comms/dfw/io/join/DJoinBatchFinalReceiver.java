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
package edu.iu.dsc.tws.comms.dfw.io.join;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.DFWIOUtils;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger;
import edu.iu.dsc.tws.comms.shuffle.Shuffle;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class DJoinBatchFinalReceiver implements MessageReceiver {

  private static final Logger LOG = Logger.getLogger(DJoinBatchFinalReceiver.class.getName());

  /**
   * Sort mergers for each target
   */
  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();

  /**
   * Comparator for sorting records
   */
  private Comparator<Object> comparator;

  private KryoSerializer kryoSerializer;

  /**
   * The receiver to be used to deliver the message
   */
  private BulkReceiver receiver;

  /**
   * The executor
   */
  protected int executor;

  /**
   * Keep the destination messages
   */
  private Map<Integer, List<Object>> targetMessagesLeft = new HashMap<>();
  private Map<Integer, List<Object>> targetMessagesRight = new HashMap<>();
  /**
   * The dataflow operation left partition
   */
  private DataFlowOperation operationLeft;

  /**
   * The dataflow operation right partition
   */
  private DataFlowOperation operationRight;

  /**
   * The lock for excluding onMessage and communicationProgress
   */
  private Lock lock = new ReentrantLock();

  /**
   * These sources called onFinished
   */
  private Map<Integer, Set<Integer>> onFinishedSourcesLeft = new HashMap<>();
  private Map<Integer, Set<Integer>> onFinishedSourcesRight = new HashMap<>();

  /**
   * The worker id
   */
  private int thisWorker;

  /**
   * sources for this operation
   */
  private Set<Integer> sources;

  private Map<Integer, Boolean> targetDone = new HashMap<>();

  /**
   * The directory in which we will be saving the shuffle objects
   */
  private String shuffleDirectory;

  /**
   * Weather everyone finished
   */
  private Set<Integer> targets = new HashSet<>();

  public DJoinBatchFinalReceiver(BulkReceiver bulkReceiver,
                                 String shuffleDir, Comparator<Object> com) {
    this.receiver = bulkReceiver;
    this.kryoSerializer = new KryoSerializer();
    this.comparator = com;
    this.shuffleDirectory = shuffleDir;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    // The init method is called twice TODO: would be better to do a complete new data flow
    // operation

    int maxBytesInMemory = DataFlowContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = DataFlowContext.getShuffleMaxRecordsInMemory(cfg);
    if (operationLeft != null) {
      this.operationRight = op;
    } else {
      executor = op.getTaskPlan().getThisExecutor();
      thisWorker = op.getTaskPlan().getThisExecutor();
      this.operationLeft = op;
      this.sources = ((DataFlowPartition) op).getSources();
      this.targets = new HashSet<>(expectedIds.keySet());

      // lists to keep track of messages for destinations
      for (int target : expectedIds.keySet()) {
        Shuffle sortedMerger = new FSKeyedSortedMerger(maxBytesInMemory, maxRecordsInMemory,
            shuffleDirectory, DFWIOUtils.getOperationName(target, operationLeft),
            operationLeft.getKeyType(), operationLeft.getDataType(), comparator, target);

        sortedMergers.put(target, sortedMerger);
        targetDone.put(target, false);
        targetMessagesLeft.put(target, new ArrayList<>());
        targetMessagesRight.put(target, new ArrayList<>());
        onFinishedSourcesLeft.put(target, new HashSet<>());
        onFinishedSourcesRight.put(target, new HashSet<>());
      }
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    throw new UnsupportedOperationException("Join operation does not support onMessage without"
        + "tag");
  }


  /**
   * This method performs the join operation based on the messages it has received
   *
   * @param source the source task
   * @param path the path that is taken by the message, that is intermediate targets
   * @param target the target of this receiver
   * @param flags the communication flags
   * @param tag tag value to identify this operation (0-left partition, 1-right partition)
   * @param object the actual message
   * @return true if message was successfully processed.
   */
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean onMessage(int source, int path, int target, int flags, int tag, Object object) {
    Shuffle sortedMerger = sortedMergers.get(target);
    if (sortedMerger == null) {
      throw new RuntimeException("Un-expected target id: " + target);
    }

    Map<Integer, List<Object>> targetMessages;
    Map<Integer, Set<Integer>> onFinishedSources;

    if (tag == 0) {
      targetMessages = targetMessagesLeft;
      onFinishedSources = onFinishedSourcesLeft;
    } else {
      targetMessages = targetMessagesRight;
      onFinishedSources = onFinishedSourcesRight;
    }

    Set<Integer> onFinishedSrcsTarget = onFinishedSources.get(target);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      if (onFinishedSrcsTarget.contains(source)) {
        LOG.log(Level.WARNING,
            String.format("%d Duplicate finish from source id %d", this.thisWorker, source));
      } else {
        onFinishedSrcsTarget.add(source);
      }
      return true;
    }

    // add the object to the map
    List<Tuple> tuples = (List<Tuple>) object;
    for (Tuple kc : tuples) {
      Object data = kc.getValue();
      byte[] d = DataSerializer.serialize(data, kryoSerializer);

      sortedMerger.add(kc.getKey(), d, d.length);
    }
    return true;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (Shuffle sorts : sortedMergers.values()) {
      sorts.run();
    }

    for (int target : targetDone.keySet()) {
      if (targetDone.get(target)) {
        continue;
      }
      lock.lock();
      try {
        if (checkIfFinished(target)) {
          Shuffle sortedMerger = sortedMergers.get(target);
          sortedMerger.switchToReading();
          Iterator<Object> itr = sortedMerger.readIterator();
          receiver.receive(target, itr);
          targetDone.put(target, true);
        } else {
          needsFurtherProgress = true;
        }
      } finally {
        lock.unlock();
      }

    }

    return needsFurtherProgress;
  }

  /**
   * checks if all the messages for this target has been received for all partitions.
   *
   * @param target target to be checked
   * @return true if all messages have been received and false otherwise
   */
  private boolean checkIfFinished(int target) {
    return operationLeft.isDelegateComplete() && operationRight.isDelegateComplete()
        && onFinishedSourcesLeft.get(target).equals(sources)
        && onFinishedSourcesRight.get(target).equals(sources);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class JoinIterator<T extends Pair> implements Iterator<Pair> {

    private Map<Object, List<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    JoinIterator(Map<Object, List<Object>> messageMap) {
      this.messageMap = messageMap;
      keyList.addAll(messageMap.keySet());
    }

    @Override
    public boolean hasNext() {
      return !keyList.isEmpty();
    }

    @Override
    public ImmutablePair next() {
      Object key = keyList.poll();
      List<Object> value = messageMap.remove(key);
      return new ImmutablePair(key, value);
    }
  }
}
