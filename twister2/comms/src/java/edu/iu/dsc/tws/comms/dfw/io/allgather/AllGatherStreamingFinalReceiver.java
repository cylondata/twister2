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

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingPartialReceiver;

public class AllGatherStreamingFinalReceiver extends GatherStreamingPartialReceiver {
  private DataFlowBroadcast broadcast;

  public AllGatherStreamingFinalReceiver(DataFlowBroadcast broadcast) {
    this.broadcast = broadcast;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
  }

  @Override
  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    return broadcast.send(task, message, flags, dest);
  }
}
