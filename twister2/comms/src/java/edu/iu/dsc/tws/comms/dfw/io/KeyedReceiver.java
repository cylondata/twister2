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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;

/**
 * This class is the abstract class that all keyed receivers extend. This provides all the basic
 * data structures and performs initialisation on those data structures.
 */
public abstract class KeyedReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(KeyedReceiver.class.getName());
  /**
   * Id of the current executor
   */
  protected int executor;

  /**
   * The buffer limit for single key. Messages are not buffered for a key after this limit is
   * reached. The limit per key is not strictly enforced and the structure may store several more
   * values past this limit for some edge cases.
   */
  protected int limitPerKey;

  /**
   * The number of keys buffered in this receiver before keys are flushed
   */
  protected int keyLimit;

  /**
   * The dataflow operation that is related to the class instance. Ex - Reduce, Gather, etc.
   */
  protected DataFlowOperation dataFlowOperation;

  /**
   * The destination identifier that defines the next destination for this receiver
   */
  protected int destination;

  /**
   * Map that keeps track of which sources have sent an finished signal. The finish signal may
   * either be a END message or a LAST message in the message flags.
   * Structure - <target, <source, true/false>>
   */
  protected Map<Integer, Map<Integer, Boolean>> finishedSources = new ConcurrentHashMap<>();


  /**
   * Map that keeps all the incoming messages to this receiver. A separate map is kept for each
   * target and witin each target values are mapped according to their key value
   * Structure - <target, <key, List<value>>>
   */
  protected Map<Integer, Map<Object, Queue<Object>>> messages = new HashMap<>();

  /**
   * Objects that are inserted into this queue are objects that can be sent out from the receiver
   * the progress method removes items from this queue and sends them. The onMessage method inserts
   * objects into this queue
   */
  protected Map<Integer, Queue<Object>> sendQueue = new HashMap<>();

  /**
   * Tracks if the partial receiver has completed processing for a given target
   */
  protected Map<Integer, Boolean> batchDone = new HashMap<>();

  /**
   * Tracks if the empty message has been sent for each target
   */
  protected Map<Integer, Boolean> isEmptySent = new HashMap<>();

  /**
   * Indicates whether the receiver instance is a final batch receiver or not. The value is set to
   * false by default. If the receiver is a final batch receiver the sendQueue data structure
   * is not used the data is always kept in the messages data structure for efficiency
   */
  protected boolean isFinalBatchReceiver = false;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();
    this.limitPerKey = 100; //TODO: use config to init this
    this.keyLimit = 10; //TODO: use config to init this

    for (Map.Entry<Integer, List<Integer>> expectedIdPerTarget : expectedIds.entrySet()) {
      Map<Integer, Boolean> finishedPerTarget = new HashMap<>();

      for (int sources : expectedIdPerTarget.getValue()) {
        finishedPerTarget.put(sources, false);
      }

      finishedSources.put(expectedIdPerTarget.getKey(), finishedPerTarget);
      messages.put(expectedIdPerTarget.getKey(), new HashMap<>());
      batchDone.put(expectedIdPerTarget.getKey(), false);
      isEmptySent.put(expectedIdPerTarget.getKey(), false);
      sendQueue.put(expectedIdPerTarget.getKey(),
          new ArrayDeque<Object>());
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean added;

    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("Executor %d, Partial receive error. Receiver did"
          + "not expect messages for this target %d", executor, target));
    }


    Map<Integer, Boolean> finishedMessages = finishedSources.get(target);

    if ((flags & MessageFlags.END) == MessageFlags.END) {
      finishedMessages.put(source, true);
      if (!isFinalBatchReceiver && isSourcesFinished(target)) {
        return moveMessagesToSendQueue(target, messages.get(target));
      }
      return true;
    }

    if (!(object instanceof Tuple) && !(object instanceof List)) {
      throw new RuntimeException(String.format("Executor %d, Partial receive error. Received"
          + " object which is not of type Tuple or List for target %d", executor, target));
    }

    added = offerMessage(target, object);

    if (added) {
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finishedMessages.put(source, true);
        //TODO: the finish of the move may not happen for LAST flags since the method to move
        //TODO: may return false
        if (!isFinalBatchReceiver && isSourcesFinished(target)) {
          moveMessagesToSendQueue(target, messages.get(target));
        }
      }
    }

    return added;
  }

  /**
   * saves the given message (or messages if the object is a list) into the messages data structure
   * if possible and rejects the message if the whole message cannot be added to the messages
   * data structure.
   *
   * @param target target for which the messages are to be added
   * @param object the message/messages to be added
   * @return true if the message was added or false otherwise
   */
  @SuppressWarnings("rawtypes")
  protected boolean offerMessage(int target, Object object) {
    Map<Object, Queue<Object>> messagesPerTarget = messages.get(target);
    if (!isFinalBatchReceiver && messagesPerTarget.size() > keyLimit) {
      LOG.fine(String.format("Executor %d Partial cannot add any further keys needs flush ",
          executor));
      moveMessagesToSendQueue(target, messagesPerTarget);
      return false;
    }

    if (object instanceof List) {
      List dataList = (List) object;
      Map<Object, List<Object>> tempList = new HashMap<>();
      for (Object dataEntry : dataList) {
        Tuple tuple = (Tuple) dataEntry;
        //If any of the keys are full the method returns false because partial objects cannot be
        //added to the messages data structure
        Object key = tuple.getKey();
        if (!isFinalBatchReceiver && messagesPerTarget.containsKey(key)
            && messagesPerTarget.get(key).size() >= limitPerKey) {
          moveMessageToSendQueue(target, messagesPerTarget, tuple.getKey());
          LOG.fine(String.format("Executor %d Partial cannot add any further values for key "
              + "needs flush ", executor));
          return false;
        }
        if (tempList.containsKey(key)) {
          tempList.get(key).add(tuple.getValue());
        } else {
          tempList.put(key, new ArrayList<>());
          tempList.get(key).add(tuple.getValue());

        }

      }
      boolean offerDone = true;
      for (Object key : tempList.keySet()) {
        if (messagesPerTarget.containsKey(key)) {
          List<Object> values = tempList.get(key);
          for (Object value : values) {
            offerDone &= messagesPerTarget.get(key).offer(value);
          }
        } else {
          ArrayDeque<Object> messagesPerKey = new ArrayDeque<>();
          List<Object> values = tempList.get(key);
          for (Object value : values) {
            offerDone &= messagesPerKey.offer(value);
          }
          messagesPerTarget.put(key, messagesPerKey);
        }
      }

      //If even one of the message offers failed we throw an exception since that message
      //cannot be recovered
      if (!offerDone) {
        throw new RuntimeException("Message lost during processing");
      }

    } else {
      Tuple tuple = (Tuple) object;
      if (messagesPerTarget.containsKey(tuple.getKey())) {
        if (messagesPerTarget.get(tuple.getKey()).size() < limitPerKey
            || isFinalBatchReceiver) {
          return messagesPerTarget.get(tuple.getKey()).offer(tuple.getValue());
        } else {
          LOG.fine(String.format("Executor %d Partial cannot add any further values for key "
              + "needs flush ", executor));
          moveMessageToSendQueue(target, messagesPerTarget, tuple.getKey());
          return false;
        }
      } else {
        ArrayDeque<Object> messagesPerKey = new ArrayDeque<>();
        messagesPerKey.add(tuple.getValue());
        messagesPerTarget.put(tuple.getKey(), messagesPerKey);
      }
    }
    return true;
  }

  /**
   * Once called this method will update the finishedSources data structure so that the given
   * source is marked as finished for each target that is present.
   *
   * @param source the task id of the source that has finished
   */
  @Override
  public void onFinish(int source) {
    for (Integer target : finishedSources.keySet()) {
      Map<Integer, Boolean> finishedMessages = finishedSources.get(target);
      finishedMessages.put(source, true);
    }
  }

  /**
   * moves all the buffered messages into the sendQueue for the given target
   *
   * @param target target for which the move needs to be done
   * @param messagesPerTarget messages for given target
   * @return true if the messagesPerTarget is not empty at the end of the moving process or false
   * otherwise
   */
  protected boolean moveMessagesToSendQueue(int target,
                                            Map<Object, Queue<Object>> messagesPerTarget) {
    Queue<Object> targetSendQueue = sendQueue.get(target);
    messagesPerTarget.entrySet().removeIf(entry -> {

      Queue<Object> entryQueue = entry.getValue();
      Object current;

      while ((current = entryQueue.peek()) != null) {
        Tuple send = new Tuple(entry.getKey(), current,
            dataFlowOperation.getKeyType(), dataFlowOperation.getDataType());

        if (targetSendQueue.offer(send)) {
          entryQueue.poll();
        } else {
          return false;
        }
      }

      return true;
    });

    return messagesPerTarget.isEmpty();
  }

  /**
   * Moves all the buffered messages for the given key into the sendQueue and removes the
   * entry in the messages data structure if all the messages are moved
   *
   * @param target target for which the move needs to be done
   * @param messagesPerTarget messages for given target
   * @param key the key to be moved
   * @return true if all the messages for that key are moved successfully
   */
  protected boolean moveMessageToSendQueue(int target, Map<Object, Queue<Object>> messagesPerTarget,
                                           Object key) {
    Queue<Object> targetSendQueue = sendQueue.get(target);
    Queue<Object> entryQueue = messagesPerTarget.get(key);
    Object current;

    while ((current = entryQueue.peek()) != null) {
      Tuple send = new Tuple(key, current,
          dataFlowOperation.getKeyType(), dataFlowOperation.getDataType());

      if (targetSendQueue.offer(send)) {
        entryQueue.poll();
      } else {
        return false;
      }
    }

    if (messagesPerTarget.get(key).isEmpty()) {
      messagesPerTarget.remove(key);
      return true;
    } else {
      return false;
    }
  }

  /**
   * checks if the sources have finished for a given target.
   *
   * @param target the target to be checked
   * @return true if all the sources for the given target are finished or false otherwise
   */
  protected boolean isSourcesFinished(int target) {
    Map<Integer, Boolean> finishedForTarget = finishedSources.get(target);
    boolean isDone = true;
    for (Boolean isSourceDone : finishedForTarget.values()) {
      isDone &= isSourceDone;
    }
    return isDone;
  }

  /**
   * checks if the Empty message was sent for this target and sends it if not sent and possible to
   * send
   *
   * @param target target for which the check is done
   * @return false if Empty is sent
   */
  protected boolean checkIfEmptyIsSent(int target) {
    boolean isSent = true;
    if (!isEmptySent.get(target)) {
      if (dataFlowOperation.isDelegateComplete() && dataFlowOperation.sendPartial(target,
          new byte[0], MessageFlags.END, destination)) {
        isEmptySent.put(target, true);
      } else {
        isSent = false;
      }
    }
    return isSent;
  }

  /**
   * Default progress method for keyed receivers. This method is targeted at partial receivers
   * which typically execute the same logic. For custom progress logic this method needs to be
   * overwritten
   */
  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    boolean isAllQueuesEmpty = false;
    for (int target : messages.keySet()) {

      //If the batch is done skip progress for this target
      if (batchDone.get(target)) {
        needsFurtherProgress = !checkIfEmptyIsSent(target);
        continue;
      }

      // now check weather we have the messages for this source to be sent
      Queue<Object> targetSendQueue = sendQueue.get(target);
      sourcesFinished = isSourcesFinished(target);

      if (!sourcesFinished && !(dataFlowOperation.isDelegateComplete()
          && messages.get(target).isEmpty() && targetSendQueue.isEmpty())) {
        needsFurtherProgress = true;
      }

      if (!targetSendQueue.isEmpty() || sourcesFinished) {
        needsFurtherProgress = sendToTarget(needsFurtherProgress, sourcesFinished, target,
            targetSendQueue);
      }

      //In reduce since we remove the key entry once we send it we only need to check if the map is
      //Empty
      isAllQueuesEmpty = isAllQueuesEmpty(targetSendQueue);
      if (!isAllQueuesEmpty) {
        needsFurtherProgress = true;
      }

      if (dataFlowOperation.isDelegateComplete() && sourcesFinished && isAllQueuesEmpty) {
        needsFurtherProgress = finishProgress(needsFurtherProgress, target);
      }
    }

    return needsFurtherProgress;
  }

  /**
   * Performs the final steps of the progress method in the receiver. If the method of finishing
   * needs to be changed this method needs to be overwritten.
   *
   * @param needsFurtherProgress current state of needsFurtherProgress value
   * @param target the target(which is a source in this instance) from which the messages are sent
   * @return true if further progress is needed or false otherwise
   */
  protected boolean finishProgress(boolean needsFurtherProgress, int target) {

    boolean needsProgress = needsFurtherProgress;
    if (dataFlowOperation.sendPartial(target, new byte[0],
        MessageFlags.END, destination)) {
      isEmptySent.put(target, true);
    } else {
      needsProgress = true;
    }
    batchDone.put(target, true);
    return needsProgress;
  }

  /**
   * checks if the queue structures used to send data is empty. If Additional data structures are
   * used this method needs to be overwritten to include them
   *
   * @param targetSendQueue message queue for the current target
   * @return true if all the related queues and structures are empty
   */
  protected boolean isAllQueuesEmpty(Queue<Object> targetSendQueue) {
    return targetSendQueue.isEmpty();
  }

  /**
   * Called from the progress method to perform the communication calls to send the queued messages
   *
   * @param needsFurtherProgress current state of needsFurtherProgress value
   * @param sourcesFinished specifies if the sources have completed
   * @param target the target(which is a source in this instance) from which the messages are sent
   * @param targetSendQueue the data structure that contains all the message data
   * @return true if further progress is needed or false otherwise
   */
  protected boolean sendToTarget(boolean needsFurtherProgress, boolean sourcesFinished, int target,
                                 Queue<Object> targetSendQueue) {
    int flags = 0;

    //Used to make sure that the code is not stuck in this while loop if the send keeps getting
    //rejected
    boolean needsProgress = needsFurtherProgress;
    boolean canProgress = true;
    Object current;
    while (canProgress && (current = targetSendQueue.peek()) != null) {
      if (sourcesFinished && targetSendQueue.size() == 1) {
        flags = MessageFlags.LAST;
      }

      if (dataFlowOperation.sendPartial(target, current, flags, destination)) {
        targetSendQueue.poll();
      } else {
        canProgress = false;
        needsProgress = true;
      }
    }
    return needsProgress;
  }
}
