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
package edu.iu.dsc.tws.executor.comms.batch;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;

public class CollectingOperation extends AbstractParallelOperation {
  public CollectingOperation(Config config, Communicator network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> srcs, int dest, EdgeGenerator e,
                      DataType dataType, String edgeName, Config config, TaskPlan taskPlan) {
    this.edgeGenerator = e;
    communicationEdge = e.generate(edgeName);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return false;
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {

  }

  @Override
  public boolean progress() {
    return false;
  }
}
