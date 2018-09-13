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
package edu.iu.dsc.tws.comms.dfw.io.reduce.keyed;

import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;

public class KeyedReduceBatchPartialReceiver extends KeyedReduceBatchReceiver {
  private static final Logger LOG = Logger.getLogger(
      KeyedReduceBatchPartialReceiver.class.getName());
  private int partialSendCount;

  public KeyedReduceBatchPartialReceiver(int dst, ReduceFunction reduce) {
    super(dst, reduce);
    this.destination = dst;
    this.reduceFunction = reduce;
    this.partialSendCount = 0;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      if (batchDone.get(target)) {
        needsFurtherProgress = !checkIfEmptyIsSent(target);
        continue;
      }
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);
      Map<Integer, Boolean> finishedForTarget = finished.get(target);

      boolean canProgress = true;
      Object currentVal = null;

      while (canProgress) {
        boolean found = true;
        boolean allFinished = true;
        boolean isAllQueuesEmpty = true;

        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
          if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
            found = false;
            canProgress = false;
          } else if (e.getValue().size() > 0) {
            moreThanOne = true;
          }
          if (!finishedForTarget.get(e.getKey())) {
            allFinished = false;
          }
        }

        // if we have queues with 0 and more than zero we need further communicationProgress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;

          //If we have got all the last messages then even if we don't get from all expected id's
          //we can flush the remaining data
          if (allFinished && dataFlowOperation.isDelegeteComplete()) {
            currentVal = reduceMessagesAll(target, messagePerTarget);
          }
        }

        if (found) {
          currentVal = reduceMessagesSingle(target, messagePerTarget);
        }

        if ((!bufferTillEnd && bufferCounts.get(target) >= bufferSize) || allFinished) {
          int flags = 0;
          if (allFinished) {
            flags = getFlags(messagePerTarget);
          }

          if (currentVal != null
              && dataFlowOperation.sendPartial(target, currentVal, flags, destination)) {
            partialSendCount++;
            // lets remove the value
            bufferCounts.put(target, 0);
            reducedValueMap.put(target, null);
            isAllQueuesEmpty = checkQueuesEmpty(messagePerTarget);

          } else {
            canProgress = false;
            needsFurtherProgress = true;
          }

          if (dataFlowOperation.isDelegeteComplete() && allFinished && isAllQueuesEmpty) {
            if (dataFlowOperation.sendPartial(target, new byte[0],
                MessageFlags.EMPTY, destination)) {
              isEmptySent.put(target, true);
            } else {
              needsFurtherProgress = true;
            }
            batchDone.put(target, true);
            // we don'target want to go through the while loop for this one
            break;
          } else {
            needsFurtherProgress = true;
          }
        }
      }
    }
    return needsFurtherProgress;
  }

  /**
   * Checks if all the queues are empty
   *
   * @param messagePerTarget data object that holds the queues of objects from each sources
   * @return true if all the message queues are empty
   */
  private boolean checkQueuesEmpty(Map<Integer, Queue<Object>> messagePerTarget) {
    boolean isAllQueuesEmpty = true;
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> value = e.getValue();
      if (value.size() != 0) {
        isAllQueuesEmpty = false;
      }
    }
    return isAllQueuesEmpty;
  }


  /**
   * Checks if this is the last message to be sent
   *
   * @param messagePerTarget data object that holds the queues of objects from each sources
   * @return the message flags to be used
   */
  private int getFlags(Map<Integer, Queue<Object>> messagePerTarget) {
    int flags = 0;
    boolean last;
    last = true;
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> valueList = e.getValue();
      if (valueList.size() > 1) {
        last = false;
      }
    }
    if (last) {
      flags = MessageFlags.FLAGS_LAST;
    }
    return flags;
  }

  /**
   * Makes a reduction for a single round. That is it only uses a single message from each expected
   * id.
   *
   * @param target target task for which the reduction is done
   * @param messagePerTarget data object that holds the queues of objects from each sources
   * @return the reduced value as an object
   */
  private Object reduceMessagesSingle(int target, Map<Integer, Queue<Object>> messagePerTarget) {
    Object currentVal;
    int tempBufferCount = bufferCounts.get(target);
    currentVal = reducedValueMap.get(target);
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> valueList = e.getValue();
      if (valueList.size() > 0) {
        if (currentVal == null) {
          currentVal = valueList.poll();
          tempBufferCount += 1;
        } else {
          Object current = valueList.poll();
          currentVal = reduceFunction.reduce(currentVal, current);
          tempBufferCount += 1;
        }
      }
    }
    bufferCounts.put(target, tempBufferCount);
    if (currentVal != null) {
      reducedValueMap.put(target, currentVal);
    }
    if (currentVal instanceof KeyedContent) {
      if (((KeyedContent) currentVal).getKey() instanceof byte[]) {
        throw new RuntimeException("Current val is bytes");
      }
    }
    return currentVal;
  }

  /**
   * Makes a reduction for a all the messages in messagePerTarget. That is it only uses a single
   * message from each expected id.
   *
   * @param target target task for which the reduction is done
   * @param messagePerTarget data object that holds the queues of objects from each sources
   * @return the reduced value as an object
   */
  private Object reduceMessagesAll(int target, Map<Integer, Queue<Object>> messagePerTarget) {
    Object currentVal;
    int tempBufferCount = bufferCounts.get(target);
    currentVal = reducedValueMap.get(target);
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> valueList = e.getValue();
      while (valueList.size() > 0) {
        if (currentVal == null) {
          currentVal = valueList.poll();
          tempBufferCount += 1;
        } else {
          Object current = valueList.poll();
          currentVal = reduceFunction.reduce(currentVal, current);
          tempBufferCount += 1;
        }
      }
    }
    bufferCounts.put(target, tempBufferCount);
    if (currentVal != null) {
      reducedValueMap.put(target, currentVal);
    }
    if (currentVal instanceof KeyedContent) {
      if (((KeyedContent) currentVal).getKey() instanceof byte[]) {
        throw new RuntimeException("Current val is bytes");
      }
    }
    return currentVal;
  }

  /**
   * checks if the Empty message was sent for this target and sends it if not sent and possible to
   * send
   *
   * @param target target for which the check is done
   * @return false if Empty is sent
   */
  private boolean checkIfEmptyIsSent(int target) {
    boolean isSent = true;
    if (!isEmptySent.get(target)) {
      if (dataFlowOperation.isDelegeteComplete() && dataFlowOperation.sendPartial(target,
          new byte[0], MessageFlags.EMPTY, destination)) {
        isEmptySent.put(target, true);
      } else {
        isSent = false;
      }
    }
    return isSent;
  }

  @Override
  public void onFinish(int source) {
    super.onFinish(source);
  }
}
