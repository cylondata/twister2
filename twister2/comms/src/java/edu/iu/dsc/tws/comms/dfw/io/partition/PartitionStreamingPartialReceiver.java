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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.dfw.io.TargetSyncReceiver;

public class PartitionStreamingPartialReceiver extends TargetSyncReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionPartialReceiver.class.getName());

  @Override
  protected boolean isAllEmpty(int target) {
    return readyToSend.get(target) == null || readyToSend.get(target).isEmpty();
  }

  @Override
  protected boolean sendSyncForward(boolean needsFurtherProgress, int target) {
    if (operation.sendPartial(representSource, new byte[0], MessageFlags.END, target)) {
      isSyncSent.put(target, true);
    } else {
      return true;
    }
    return needsFurtherProgress;
  }

  @Override
  protected boolean sendToTarget(int target, boolean sync) {
    List<Object> sendList = readyToSend.get(target);

    if (sendList == null || sendList.isEmpty()) {
      return false;
    }

    // if we send this list successfully
    if (operation.sendPartial(representSource, sendList, 0, target)) {
      // lets remove from ready list and clear the list
      sendList.clear();
      return true;
    }

    return false;
  }

  @Override
  protected boolean aggregate(int target, boolean sync) {
    swapToReady(target, messages.get(target));
    return true;
  }

  @Override
  protected boolean isFilledToSend(int target, boolean sync) {
    return readyToSend.get(target) != null && readyToSend.get(target).size() > 0;
  }
}
