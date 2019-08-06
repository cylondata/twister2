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

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.dfw.TreeBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.DFWIOUtils;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.gather.BaseGatherBatchReceiver;

public class AllGatherBatchFinalReceiver extends BaseGatherBatchReceiver {
  private static final Logger LOG = Logger.getLogger(AllGatherBatchFinalReceiver.class.getName());

  private TreeBroadcast gatherReceiver;

  public AllGatherBatchFinalReceiver(TreeBroadcast bCast) {
    this.gatherReceiver = bCast;
  }

  @Override
  protected boolean sendSyncForward(boolean needsFurtherProgress, int target) {
    return DFWIOUtils.sendFinalSyncForward(needsFurtherProgress, target, syncState,
        barriers, gatherReceiver, isSyncSent);
  }

  @Override
  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    if (gatherReceiver.send(task, gatheredValuesMap.get(task), flags)) {
      gatheredValuesMap.remove(task);
      onFinish(task);
    } else {
      return false;
    }
    return true;
  }

  @Override
  protected boolean isFilledToSend(int target, boolean sync) {
    if (!sync) {
      return false;
    }
    boolean b = targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && gatheredValuesMap.get(target) != null && gatheredValuesMap.get(target).size() > 0;
    if (b) {
      LOG.info("Gather ready: " + b);
    }
    return b;
  }
}
