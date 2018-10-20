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
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;

public class JoinBatchFinalReceiver implements MessageReceiver {

  /**
   * The receiver to be used to deliver the message
   */
  private BulkReceiver receiver;

  public JoinBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.receiver = bulkReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    throw new UnsupportedOperationException("Join operation does not support onMessage without"
        + "tag");
  }


  /**
   * This method performs the join operation based on the messages it has received
   *
   * @param source the source task
   * @param path the path that is taken by the message, that is intermediate targets
   * @param target the target of this receiver
   * @param flags the communication flags
   * @param tag tag value to identify this operation (0-left partition, 1-right partition)
   * @param object the actual message
   * @return true if message was successfully processed.
   */
  @Override
  public boolean onMessage(int source, int path, int target, int flags, int tag, Object object) {
    return false;
  }

  @Override
  public boolean progress() {
    return false;
  }
}
