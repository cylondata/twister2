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
package edu.iu.dsc.tws.api.task.ops;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.graph.Edge;

public abstract class AbstractKeyedOpsConfig<T extends AbstractOpsConfig>
    extends AbstractOpsConfig<T> {

  private DataType opKeyType = DataType.OBJECT;

  protected AbstractKeyedOpsConfig(String parent,
                                   String operationName,
                                   ComputeConnection computeConnection) {
    super(parent, operationName, computeConnection);
  }

  public T withKeyType(DataType keyType) {
    this.opKeyType = keyType;
    return (T) this;
  }

  @Override
  Edge buildEdge() {
    Edge edge = super.buildEdge();
    edge.setKeyed(true);
    edge.setKeyType(opKeyType);
    return edge;
  }
}
