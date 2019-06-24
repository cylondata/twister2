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
package edu.iu.dsc.tws.task.impl.ops;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.task.TaskPartitioner;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.task.impl.ComputeConnection;

public abstract class AbstractKeyedOpsConfig<T extends AbstractOpsConfig>
    extends AbstractOpsConfig<T> {

  protected MessageType opKeyType = MessageTypes.OBJECT;
  protected TaskPartitioner tPartitioner;

  protected AbstractKeyedOpsConfig(String parent,
                                   String operationName,
                                   ComputeConnection computeConnection) {
    super(parent, operationName, computeConnection);
  }

  public <C> T withTaskPartitioner(Class<C> tClass, TaskPartitioner<C> taskPartitioner) {
    this.tPartitioner = taskPartitioner;
    return (T) this;
  }

  public T withTaskPartitioner(TaskPartitioner taskPartitioner) {
    this.tPartitioner = taskPartitioner;
    return (T) this;
  }

  public T withKeyType(MessageType keyType) {
    this.opKeyType = keyType;
    return (T) this;
  }

  @Override
  Edge buildEdge() {
    Edge edge = super.buildEdge();
    edge.setKeyed(true);
    edge.setKeyType(opKeyType);
    edge.setPartitioner(this.tPartitioner);
    return edge;
  }
}
