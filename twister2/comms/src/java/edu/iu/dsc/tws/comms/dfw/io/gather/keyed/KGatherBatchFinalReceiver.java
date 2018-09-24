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

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;

/**
 * Created by pulasthi on 9/20/18.
 */
public class KGatherBatchFinalReceiver extends KeyedReceiver {
  private static final Logger LOG = Logger.getLogger(KGatherBatchFinalReceiver.class.getName());

  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  public KGatherBatchFinalReceiver(BulkReceiver receiver,
                                   int limitPerKey) {
    this.bulkReceiver = receiver;
    this.limitPerKey = limitPerKey;
    this.isFinalReceiver = true;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    for (int target : messages.keySet()) {

      if (batchDone.get(target)) {
        continue;
      }

      sourcesFinished = isSourcesFinished(target);
      if (!sourcesFinished && !(dataFlowOperation.isDelegeteComplete()
          && messages.get(target).isEmpty())) {
        needsFurtherProgress = true;
      }

      if (sourcesFinished && dataFlowOperation.isDelegeteComplete()) {
        batchDone.put(target, true);
        //TODO: check if we can simply remove the data, that is use messages.remove()
        //bulkReceiver.receive(target, messages.get(target));
      }


    }

    return needsFurtherProgress;
  }
}
