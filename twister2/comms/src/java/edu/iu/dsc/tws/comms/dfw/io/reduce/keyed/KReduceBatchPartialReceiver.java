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
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;

/**
 * Keyed reduce receiver for batch mode
 */
public class KReduceBatchPartialReceiver extends KeyedReceiver {
  private static final Logger LOG = Logger.getLogger(KReduceBatchPartialReceiver.class.getName());

  /**
   * The function that is used for the reduce operation
   */
  protected ReduceFunction reduceFunction;

  public KReduceBatchPartialReceiver(int dest, ReduceFunction function) {
    this.reduceFunction = function;
    this.destination = dest;
    this.limitPerKey = 1;
  }

  /**
   * The reduce operation overrides the offer method because the reduce operation
   * does not save all the incomming messages, it rather reduces messages with the same key and
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
      needsFlush = true;
      LOG.fine(String.format("Executor %d Partial cannot add any further keys needs flush ",
          executor));
      return false;
    }
    Object currentEntry;
    if (object instanceof List) {
      List dataList = (List) object;
      for (Object dataEntry : dataList) {
        KeyedContent keyedContent = (KeyedContent) dataEntry;
        Object key = keyedContent.getKey();
        if (messagesPerTarget.containsKey(key)) {
          messagesPerTarget.put(key, new ArrayBlockingQueue<>(limitPerKey));
          messagesPerTarget.get(keyedContent.getKey()).add(keyedContent.getValue());
        } else {
          currentEntry = messagesPerTarget.get(keyedContent.getKey()).poll();
          currentEntry = reduceFunction.reduce(currentEntry, keyedContent.getValue());
          messagesPerTarget.get(keyedContent.getKey()).add(currentEntry);
        }

      }
    } else {
      KeyedContent keyedContent = (KeyedContent) object;
      Object key = keyedContent.getKey();
      if (messagesPerTarget.containsKey(key)) {
        messagesPerTarget.put(key, new ArrayBlockingQueue<>(limitPerKey));
        messagesPerTarget.get(keyedContent.getKey()).add(keyedContent.getValue());
      } else {
        currentEntry = messagesPerTarget.get(keyedContent.getKey()).poll();
        currentEntry = reduceFunction.reduce(currentEntry, keyedContent.getValue());
        messagesPerTarget.get(keyedContent.getKey()).add(currentEntry);
      }
    }
    return true;
  }

  @Override
  public boolean progress() {
    return false;
  }
}
