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
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.TaskPartitioner;
import edu.iu.dsc.tws.task.graph.Edge;

public class KeyedReduceConfig extends AbstractKeyedOpsConfig<KeyedReduceConfig> {

  private IFunction redFunction;
  private Op op;

  protected KeyedReduceConfig(String parent, ComputeConnection computeConnection) {
    super(parent, OperationNames.KEYED_REDUCE, computeConnection);
  }

  /**
   * @param tClass Class of {@link IFunction} arguments
   */
  public <T> KeyedReduceConfig withReductionFunction(Class<T> tClass,
                                                     IFunction<T> reductionFunction) {
    this.redFunction = reductionFunction;
    return this;
  }

  public KeyedReduceConfig withReductionFunction(IFunction reductionFunction) {
    this.redFunction = reductionFunction;
    return this;
  }

  public KeyedReduceConfig withOperation(Op operation, DataType dataType) {
    this.op = operation;
    return this.withDataType(dataType);
  }

  @Override
  void validate() {
    ReduceConfig.validateReduce(this.redFunction, this.op, this.getOpDataType());
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    ReduceConfig.updateReduceEdge(newEdge, this.redFunction, this.op, this.getOpDataType());
    return newEdge;
  }
}
