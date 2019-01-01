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
package edu.iu.dsc.tws.comms.dfw.io.gather;

import edu.iu.dsc.tws.comms.api.BulkReceiver;

public class GatherBatchFinalReceiver extends BaseGatherBatchFinalReceiver {
  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private BulkReceiver bulkReceiver;

  public GatherBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.bulkReceiver = bulkReceiver;
  }

  @Override
  protected void init() {
    bulkReceiver.init(config, expIds.keySet());
  }

  @Override
  protected void handleFinish(int t) {
    batchDone.put(t, true);
    bulkReceiver.receive(t, finalMessages.get(t).iterator());
    // we can call on finish at this point
    onFinish(t);
  }
}
