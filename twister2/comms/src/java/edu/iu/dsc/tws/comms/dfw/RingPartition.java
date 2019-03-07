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
package edu.iu.dsc.tws.comms.dfw;

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;

public class RingPartition implements DataFlowOperation {

  public RingPartition(Config cfg, TWSChannel channel, TaskPlan tPlan, Set<Integer> sources,
                           Set<Integer> targets, MessageReceiver finalRcvr,
                           MessageReceiver partialRcvr,
                           MessageType dType, MessageType rcvType,
                           MessageType kType, MessageType rcvKType,
                           int e) {

  }


  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    return false;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return false;
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return false;
  }

  @Override
  public boolean progress() {
    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public void clean() {

  }

  @Override
  public TaskPlan getTaskPlan() {
    return null;
  }

  @Override
  public String getUniqueId() {
    return null;
  }
}
