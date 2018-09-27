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

  /**
   * checks if the queue structures used to send data is empty. If Additional data structures are
   * used this method needs to be overwritten to include them. This method overrides the default
   * implementation and also checks to make sure the sendList is also empty
   *
   * @param targetSendQueue message queue for the current target
   * @return true if all the related queues and structures are empty
   */
  @Override
  protected boolean isAllQueuesEmpty(Queue<Object> targetSendQueue) {
    return targetSendQueue.isEmpty() && sendList.isEmpty();
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
  @Override
  protected boolean sendToTarget(boolean needsFurtherProgress, boolean sourcesFinished, int target,
                                 Queue<Object> targetSendQueue) {
    // We only try to send new messages if the sendList is empty. If it still has values
    // that means a previous senPartial call returned false. so we need to first send that
    // data before processing new data
    boolean needsProgress = needsFurtherProgress;
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
        needsProgress = true;
      }
    }
    return needsProgress;
  }
}
