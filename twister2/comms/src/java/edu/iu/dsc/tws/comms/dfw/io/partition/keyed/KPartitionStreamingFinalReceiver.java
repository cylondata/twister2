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
package edu.iu.dsc.tws.comms.dfw.io.partition.keyed;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.partition.BasePartitionStreamingFinalReceiver;

public class KPartitionStreamingFinalReceiver extends BasePartitionStreamingFinalReceiver
    implements MessageReceiver {

  private SingularReceiver receiver;

  public KPartitionStreamingFinalReceiver(SingularReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, operation, expectedIds);
    receiver.init(cfg, expectedIds.keySet());
  }

  @Override
  public boolean receive(int target, Object message) {
    if (message instanceof Tuple) {
      return receiver.receive(target, message);
    }
    throw new RuntimeException("Expecting a tuple object: " + message.getClass());
  }
}
