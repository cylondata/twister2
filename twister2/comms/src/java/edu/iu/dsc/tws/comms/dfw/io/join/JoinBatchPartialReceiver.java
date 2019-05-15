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
package edu.iu.dsc.tws.comms.dfw.io.join;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;

/**
 * Final receiver for partitions in join operations, each Join final receiver forwards the messages
 * to the TODO add more info
 */
public class JoinBatchPartialReceiver implements MessageReceiver {

  /**
   * denotes the order of the related partition in the join operation, For example in a simple
   * join operation tag of 0 is the left partition and tag of 1 is right partition.
   */
  private int tag;

  /**
   * The final receiver for the whole join operation where the actual local join will occur.
   */
  private MessageReceiver joiningReceiver;

  public JoinBatchPartialReceiver(int tag, MessageReceiver joiningReceiver) {
    this.tag = tag;
    this.joiningReceiver = joiningReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    joiningReceiver.init(cfg, op, expectedIds);
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    return joiningReceiver.onMessage(source, path, target, flags, tag, object);
  }

  @Override
  public boolean progress() {
    return joiningReceiver.progress();
  }
}
