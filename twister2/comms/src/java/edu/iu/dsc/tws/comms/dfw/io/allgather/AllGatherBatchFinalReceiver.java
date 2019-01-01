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
package edu.iu.dsc.tws.comms.dfw.io.allgather;

import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.gather.BaseGatherBatchFinalReceiver;

public class AllGatherBatchFinalReceiver extends BaseGatherBatchFinalReceiver {
  private DataFlowBroadcast gatherReceiver;

  public AllGatherBatchFinalReceiver(DataFlowBroadcast bCast) {
    this.gatherReceiver = bCast;
  }

  @Override
  protected void init() {
  }

  @Override
  protected void handleFinish(int t) {
    if (gatherReceiver.send(t, finalMessages.get(t), 0)) {
      batchDone.put(t, true);
      onFinish(t);
    }
  }
}
