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

import java.util.Iterator;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.utils.JoinRelation;

public class BulkReceiverWrapper implements BulkReceiver {

  private JoinRelation joinRelation;
  private JoinBatchCombinedReceiver joinBatchCombinedReceiver;

  public BulkReceiverWrapper(JoinBatchCombinedReceiver joinBatchCombinedReceiver,
                             JoinRelation joinRelation) {
    this.joinRelation = joinRelation;
    this.joinBatchCombinedReceiver = joinBatchCombinedReceiver;
  }

  public static BulkReceiverWrapper wrap(JoinBatchCombinedReceiver joinBatchCombinedReceiver,
                                         JoinRelation joinRelation) {
    return new BulkReceiverWrapper(joinBatchCombinedReceiver, joinRelation);
  }

  @Override
  public void init(Config cfg, Set<Integer> targets) {
    this.joinBatchCombinedReceiver.init(cfg, targets);
  }

  @Override
  public boolean receive(int target, Iterator<Object> it) {
    return this.joinBatchCombinedReceiver.receive(target, it, this.joinRelation);
  }

  @Override
  public boolean sync(int target, byte[] message) {
    return this.joinBatchCombinedReceiver.sync(target, message, this.joinRelation);
  }
}
