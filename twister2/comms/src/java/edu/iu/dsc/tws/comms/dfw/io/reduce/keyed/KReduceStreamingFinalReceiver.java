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

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;

/**
 * Keyed reduce final receiver for streaming  mode
 */
public class KReduceStreamingFinalReceiver extends KReduceStreamingReceiver {
  private static final Logger LOG = Logger.getLogger(KReduceStreamingFinalReceiver.class.getName());

  /**
   * Final receiver that get the reduced values for the operation
   */
  private SingularReceiver singularReceiver;

  public KReduceStreamingFinalReceiver(ReduceFunction reduce, SingularReceiver receiver,
                                       int window) {
    this.reduceFunction = reduce;
    this.singularReceiver = receiver;
    this.limitPerKey = 1;
    this.isFinalReceiver = true;
    this.windowSize = window;
    this.localWindowCount = 0;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    for (int target : messages.keySet()) {

      if (batchDone.get(target)) {
        continue;
      }

      BlockingQueue<Object> targetSendQueue = sendQueue.get(target);
      sourcesFinished = isSourcesFinished(target);
      if (!sourcesFinished && !(dataFlowOperation.isDelegeteComplete()
          && messages.get(target).isEmpty())) {
        needsFurtherProgress = true;
      }

      if (!targetSendQueue.isEmpty()) {
        Object current;
        while ((current = targetSendQueue.poll()) != null) {
          singularReceiver.receive(target, current);
        }
      }

      if (sourcesFinished && dataFlowOperation.isDelegeteComplete()
          && targetSendQueue.isEmpty()) {
        batchDone.put(target, true);
      }


    }

    return needsFurtherProgress;
  }
}
