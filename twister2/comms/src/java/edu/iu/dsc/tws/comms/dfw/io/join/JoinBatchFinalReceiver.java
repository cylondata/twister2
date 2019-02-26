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
import java.util.Collection;
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
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public class JoinBatchFinalReceiver implements MessageReceiver {

  private static final Logger LOG = Logger.getLogger(JoinBatchFinalReceiver.class.getName());

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

  public JoinBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.receiver = bulkReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    // The init method is called twice TODO: would be better to do a complete new data flow
    // operation
    if (operationLeft != null) {
      this.operationRight = op;
    } else {
      executor = op.getTaskPlan().getThisExecutor();
      thisWorker = op.getTaskPlan().getThisExecutor();
      this.operationLeft = op;
      this.sources = ((DataFlowPartition) op).getSources();

      // lists to keep track of messages for destinations
      for (int target : expectedIds.keySet()) {
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
  public boolean onMessage(int source, int path, int target, int flags, int tag, Object object) {

    if (tag != 0 && tag != 1) {
      throw new RuntimeException("Tag value must be either 0(left) or 1(right) for join operation");
    }

    lock.lock();
    Map<Integer, List<Object>> targetMessages;
    Map<Integer, Set<Integer>> onFinishedSources;

    if (tag == 0) {
      targetMessages = targetMessagesLeft;
      onFinishedSources = onFinishedSourcesLeft;
    } else {
      targetMessages = targetMessagesRight;
      onFinishedSources = onFinishedSourcesRight;
    }

    try {
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

      List<Object> targetMsgList = targetMessages.get(target);
      if (targetMsgList == null) {
        throw new RuntimeException(String.format("%d target not exisits %d", executor, target));
      }
      if (object instanceof List) {
        targetMsgList.addAll((Collection<?>) object);
      } else {
        targetMsgList.add(object);
      }
    } finally {
      lock.unlock();
    }
    return true;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : targetDone.keySet()) {
      if (targetDone.get(target)) {
        continue;
      }
      lock.lock();
      try {
        if (checkIfFinished(target)) {
          Map<Object, List<Object>> results = innerJoin(targetMessagesLeft.get(target),
              targetMessagesRight.get(target));
          receiver.receive(target, new JoinIterator(results));
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
   * Performs an inner join on the two data lists that have been provided.
   * The join is performed using the key value.
   *
   * @param left left partition of the join
   * @param right right partition of the join
   * @return the joined list of values
   */
  private Map<Object, List<Object>> innerJoin(List<Object> left, List<Object> right) {
    Map<Object, List<Object>> joined = new HashMap<>();
    for (Object entry : left) {
      Object key = ((Tuple) entry).getKey();
      if (joined.containsKey(key)) {
        joined.get(key).add(((Tuple) entry).getValue());
      } else {
        joined.put(key, new ArrayList<Object>());
        joined.get(key).add(((Tuple) entry).getValue());
      }
    }

    for (Object entry : right) {
      Object key = ((Tuple) entry).getKey();
      if (joined.containsKey(key)) {
        joined.get(key).add(((Tuple) entry).getValue());
      } else {
        joined.put(key, new ArrayList<Object>());
        joined.get(key).add(((Tuple) entry).getValue());
      }
    }
    return joined;
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
