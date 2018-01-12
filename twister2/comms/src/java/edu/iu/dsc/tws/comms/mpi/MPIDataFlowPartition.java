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
package edu.iu.dsc.tws.comms.mpi;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class MPIDataFlowPartition implements DataFlowOperation {

  @Override
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {

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
  public boolean send(int source, Object message, int flags, int path) {
    return false;
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int path) {
    return false;
  }

  @Override
  public void progress() {

  }

  @Override
  public void close() {

  }

  @Override
  public void finish() {

  }

  @Override
  public MessageType getType() {
    return null;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return null;
  }
}
