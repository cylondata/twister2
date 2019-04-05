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

import edu.iu.dsc.tws.comms.api.MessageFlags;

public class GatherBatchPartialReceiver extends BaseGatherBatchReceiver {
  public GatherBatchPartialReceiver(int target) {
  }

  @Override
  protected boolean sendSyncForward(boolean needsFurtherProgress, int target) {
    if (operation.sendPartial(target, new byte[0], MessageFlags.END, destination)) {
      LOG.info(String.format("Sent sync forward %d", target));
      isSyncSent.put(target, true);
    } else {
      return true;
    }
    return needsFurtherProgress;
  }

  @Override
  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    return operation.sendPartial(task, message, flags, dest);
  }

  @Override
  protected boolean isFilledToSend(int target, boolean sync) {
    return gatheredValuesMap.get(target) != null && gatheredValuesMap.get(target).size() > 0;
  }

}

