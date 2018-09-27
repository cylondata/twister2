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

package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;

/**
 * Abstract class that is extended by keyed reduce batch receivers
 */
public class KGatherBatchPartialReceiver extends KeyedReceiver {
  private static final Logger LOG = Logger.getLogger(KGatherBatchPartialReceiver.class.getName());

  /**
   * List used to send data from the partial receiver. This is used because once we take elements
   * from the send queue we cannot put them back in if the send fails. So the send messages are
   * kept in the variable until the send method returns true.
   */
  private List<Object> sendList = new ArrayList<>();

  /**
   * Flags associated with the current sendList
   */
  private int flags = 0;

  public KGatherBatchPartialReceiver(int dest, int limitPerKey) {
    this.destination = dest;
    this.limitPerKey = limitPerKey;
  }

  @Override
  @SuppressWarnings("unchecked")
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

      if (!sourcesFinished && !(dataFlowOperation.isDelegeteComplete()
          && messages.get(target).isEmpty() && targetSendQueue.isEmpty())) {
        needsFurtherProgress = true;
      }

      if (!targetSendQueue.isEmpty() || sourcesFinished) {

        // We only try to send new messages if the sendList is empty. If it still has values
        // that means a previous senPartial call returned false. so we need to first send that
        // data before processing new data
        if (sendList.isEmpty()) {
          while (!targetSendQueue.isEmpty()) {
            if (sourcesFinished && targetSendQueue.size() == 1) {
              flags = MessageFlags.LAST;
            }
            sendList.add(targetSendQueue.poll());
          }
        }

        if (!sendList.isEmpty()) {
          if (dataFlowOperation.sendPartial(target, sendList, flags, destination)) {
            System.out.println("Sent Partial executor : " + executor + "size" + sendList.size());
            sendList = new ArrayList<>();
            flags = 0;
          } else {
            needsFurtherProgress = true;
          }
        }
      }

      //In reduce since we remove the key entry once we send it we only need to check if the map is
      //Empty
      isAllQueuesEmpty = targetSendQueue.isEmpty() && sendList.isEmpty();
      if (!isAllQueuesEmpty) {
        needsFurtherProgress = true;
      }

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
      }
    }

    return needsFurtherProgress;
  }
}
