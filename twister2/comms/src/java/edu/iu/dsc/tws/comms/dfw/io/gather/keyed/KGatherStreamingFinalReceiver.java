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

import edu.iu.dsc.tws.comms.api.BulkReceiver;

/**
 * Keyed reduce final receiver for streaming  mode
 */
public class KGatherStreamingFinalReceiver extends KGatherStreamingReceiver {
  private static final Logger LOG = Logger.getLogger(KGatherStreamingFinalReceiver.class.getName());

  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

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


  public KGatherStreamingFinalReceiver(BulkReceiver receiver,
                                       int window) {
    this.bulkReceiver = receiver;
    this.limitPerKey = 1;
    this.windowSize = window;
    this.localWindowCount = 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    for (int target : messages.keySet()) {

      if (batchDone.get(target)) {
        continue;
      }

      Queue<Object> targetSendQueue = sendQueue.get(target);
      sourcesFinished = isSourcesFinished(target);
      if (!sourcesFinished && !(dataFlowOperation.isDelegateComplete()
          && messages.get(target).isEmpty())) {
        needsFurtherProgress = true;
      }

      if (!targetSendQueue.isEmpty()) {
        List<Object> results = new ArrayList<Object>();
        Object current;
        while ((current = targetSendQueue.poll()) != null) {
          results.add(current);
        }
        bulkReceiver.receive(target, results.iterator());
      }

      if (sourcesFinished && dataFlowOperation.isDelegateComplete()
          && targetSendQueue.isEmpty()) {
        batchDone.put(target, true);
      }
    }

    return needsFurtherProgress;
  }
}
