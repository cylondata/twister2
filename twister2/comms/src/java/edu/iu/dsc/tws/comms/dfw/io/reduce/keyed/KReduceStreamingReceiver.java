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
package edu.iu.dsc.tws.comms.dfw.io.reduce.keyed;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;

/**
 * Abstract class that is extended by keyed reduce streaming receivers
 */
public abstract class KReduceStreamingReceiver extends KeyedReceiver {
  private static final Logger LOG = Logger.getLogger(KReduceStreamingReceiver.class.getName());


  /**
   * The function that is used for the reduce operation
   */
  protected ReduceFunction reduceFunction;

  /**
   * Streaming messages are only kept until the window size is met. by default the window size
   * is 1, so all messages are forwarded as they arrive.
   */
  protected int windowSize = 1;

  /**
   * variable used to keep track of the current local window count. This value is always reset
   * to 0 after it reaches the windowSize
   */
  protected int localWindowCount;

  /**
   * The reduce operation overrides the offer method because the reduce operation
   * does not save all the incoming messages, it rather reduces messages with the same key and
   * saves only the reduced values. So the messages data structure will only have a single
   * entry for each target, key pair
   *
   * @param target target for which the messages are to be added
   * @param object the message/messages to be added
   * @return true if the message was added or false otherwise
   */
  @Override
  @SuppressWarnings("rawtypes")
  protected boolean offerMessage(int target, Object object) {


    Map<Object, Queue<Object>> messagesPerTarget = messages.get(target);

    if (messagesPerTarget.size() > keyLimit) {
      LOG.fine(String.format("Executor %d Partial cannot add any further keys needs flush ",
          executor));
      moveMessagesToSendQueue(target, messagesPerTarget);
      return false;
    }
    if (object instanceof List) {
      List dataList = (List) object;
      for (Object dataEntry : dataList) {
        KeyedContent keyedContent = (KeyedContent) dataEntry;
        if (!reduceAndInsert(messagesPerTarget, keyedContent)) {
          throw new RuntimeException("Reduce operation should not fail to insert key");
        }
        this.localWindowCount++;
      }
    } else {
      KeyedContent keyedContent = (KeyedContent) object;
      if (!reduceAndInsert(messagesPerTarget, keyedContent)) {
        throw new RuntimeException("Reduce operation should not fail to insert key");
      }
      this.localWindowCount++;
    }

    if (localWindowCount > windowSize) {
      if (moveMessagesToSendQueue(target, messagesPerTarget)) {
        //TODO: what if the move returns false, do we still set the localWindowCount to zero?
        localWindowCount = 0;
      }
    }
    return true;
  }

  /**
   * reduces the given KeyedContent value with the existing value in the messages for the same key.
   * If the key is not present it will insert the key with the given value.
   *
   * @param messagesPerTarget messages for the current target
   * @param keyedContent value to be reduced and inserted
   */
  private boolean reduceAndInsert(Map<Object, Queue<Object>> messagesPerTarget,
                                  KeyedContent keyedContent) {
    Object currentEntry;
    Object key = keyedContent.getKey();
    if (!messagesPerTarget.containsKey(key)) {
      messagesPerTarget.put(key, new ArrayBlockingQueue<>(limitPerKey));
      return messagesPerTarget.get(keyedContent.getKey()).offer(keyedContent.getValue());
    } else {
      currentEntry = messagesPerTarget.get(keyedContent.getKey()).poll();
      currentEntry = reduceFunction.reduce(currentEntry, keyedContent.getValue());
      return messagesPerTarget.get(keyedContent.getKey()).offer(currentEntry);
    }
  }

  /**
   * moves all the buffered messages into the sendQueue for the given target, this method assumes
   * that for each target that there is only one object in the queue. This is required when working
   * with reduce operations
   *
   * @param target target for which the move needs to be done
   * @return true if the messagesPerTarget is not empty at the end of the moving process or false
   * otherwise
   */
  @Override
  protected boolean moveMessagesToSendQueue(int target,
                                            Map<Object, Queue<Object>> messagesPerTarget) {
    BlockingQueue<Object> targetSendQueue = sendQueue.get(target);
    messagesPerTarget.entrySet().removeIf(entry -> {
      KeyedContent send = new KeyedContent(entry.getKey(), entry.getValue().peek(),
          dataFlowOperation.getKeyType(), dataFlowOperation.getDataType());
      return targetSendQueue.offer(send);
    });

    return messagesPerTarget.isEmpty();
  }
}
