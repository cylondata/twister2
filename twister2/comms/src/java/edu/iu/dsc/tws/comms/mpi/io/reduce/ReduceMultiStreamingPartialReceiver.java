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
package edu.iu.dsc.tws.comms.mpi.io.reduce;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;

public class ReduceMultiStreamingPartialReceiver implements MultiMessageReceiver {
  private ReduceFunction reduceFunction;

  private ReduceReceiver reduceReceiver;

  private Map<Integer, ReduceStreamingPartialReceiver> receiverMap = new HashMap<>();

  public ReduceMultiStreamingPartialReceiver(ReduceFunction reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op,
                   Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
    for (Map.Entry<Integer, Map<Integer, List<Integer>>> e : expectedIds.entrySet()) {
      ReduceStreamingPartialReceiver finalReceiver =
          new ReduceStreamingPartialReceiver(e.getKey(), reduceFunction);
      receiverMap.put(e.getKey(), finalReceiver);
      finalReceiver.init(cfg, op, e.getValue());
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    ReduceStreamingPartialReceiver finalReceiver = receiverMap.get(path);
    return finalReceiver.onMessage(source, path, target, flags, object);
  }

  @Override
  public void progress() {
    for (Map.Entry<Integer, ReduceStreamingPartialReceiver> e : receiverMap.entrySet()) {
      e.getValue().progress();
    }
  }
}
