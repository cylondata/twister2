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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;
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
        reduceAndInsert(messagesPerTarget, keyedContent);

      }
    } else {
      KeyedContent keyedContent = (KeyedContent) object;
      reduceAndInsert(messagesPerTarget, keyedContent);
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
  private void reduceAndInsert(Map<Object, Queue<Object>> messagesPerTarget,
                               KeyedContent keyedContent) {
    Object currentEntry;
    Object key = keyedContent.getKey();
    if (!messagesPerTarget.containsKey(key)) {
      messagesPerTarget.put(key, new ArrayBlockingQueue<>(limitPerKey));
      messagesPerTarget.get(keyedContent.getKey()).add(keyedContent.getValue());
    } else {
      currentEntry = messagesPerTarget.get(keyedContent.getKey()).poll();
      currentEntry = reduceFunction.reduce(currentEntry, keyedContent.getValue());
      messagesPerTarget.get(keyedContent.getKey()).add(currentEntry);
    }
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    boolean isAllQueuesEmpty = false;
    for (int target : messages.keySet()) {

      //If the batch is done skip progress for this target
      if (batchDone.get(target)) {
        //needsFurtherProgress = !checkIfEmptyIsSent(target);
        continue;
      }

      // now check weather we have the messages for this source
      Map<Object, Queue<Object>> messagePerTarget = messages.get(target);
      sourcesFinished = isSourcesFinished(target);

      if (!sourcesFinished) {
        needsFurtherProgress = true;
      }

      if (needsFlush || sourcesFinished) {
        int flags = 0;

        //In reduce a flush is only needed when the number of keys exceed the key limit
        //So we flush the keys and remove them from the messages list.This would be the same
        //when the sources are finished.

        Iterator<Map.Entry<Object, Queue<Object>>> iter = messagePerTarget.entrySet().iterator();
        while (iter.hasNext()) {

          Map.Entry<Object, Queue<Object>> current = iter.next();
          Object key = current.getKey();
          if (sourcesFinished && messagePerTarget.size() == 1) {
            flags = MessageFlags.LAST;
          }
          //try to send the message
          Object value = current.getValue().poll();
          if (value != null) {
            KeyedContent sendData = new KeyedContent(key, value, dataFlowOperation.getKeyType(),
                dataFlowOperation.getDataType());
            if (dataFlowOperation.sendPartial(target, sendData, flags, destination)) {
              iter.remove();
              needsFlush = false;
            } else {
              needsFurtherProgress = true;
            }
          }
        }


      }

      //In reduce since we remove the key entry once we send it we only need to check if the map is
      //Empty
      isAllQueuesEmpty = messagePerTarget.isEmpty();

      if (dataFlowOperation.isDelegeteComplete() && sourcesFinished && isAllQueuesEmpty) {
        if (dataFlowOperation.sendPartial(target, new byte[0],
            MessageFlags.END, destination)) {
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

    return needsFurtherProgress;
  }

  /**
   * checks if the sources have finished for a given target.
   *
   * @param target the target to be checked
   * @return true if all the sources for the given target are finished or false otherwise
   */
  private boolean isSourcesFinished(int target) {
    Map<Integer, Boolean> finishedForTarget = finishedSources.get(target);
    boolean isDone = true;
    for (Boolean isSourceDone : finishedForTarget.values()) {
      isDone &= isSourceDone;
    }
    return isDone;
  }

}
