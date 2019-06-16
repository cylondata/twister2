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
import edu.iu.dsc.tws.api.task.function.ReduceFn;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.graph.Edge;

public class ReduceConfig extends AbstractOpsConfig<ReduceConfig> {

  private IFunction redFunction;
  private Op op;

  public ReduceConfig(String source, ComputeConnection computeConnection) {
    this(source, OperationNames.REDUCE, computeConnection);
  }

  protected ReduceConfig(String source,
                         String operationName,
                         ComputeConnection computeConnection) {
    super(source, operationName, computeConnection);
  }

  /**
   * Define a function to handle the reduction
   *
   * @param tClass Class of {@link IFunction} arguments
   */
  public <T> ReduceConfig withReductionFunction(Class<T> tClass,
                                                IFunction<T> reductionFunction) {
    this.redFunction = reductionFunction;
    return this;
  }

  public ReduceConfig withReductionFunction(IFunction reductionFunction) {
    this.redFunction = reductionFunction;
    return this;
  }

  public ReduceConfig withOperation(Op operation, DataType dataType) {
    this.op = operation;
    return this.withDataType(dataType);
  }

  public static void validateReduce(IFunction reductionFunction,
                                    Op operation,
                                    DataType dataType) {
    if (reductionFunction == null && operation == null) {
      failValidation("Either reduction function or Operation "
          + "should be specified when declaring reduce operations.");
    }

    if (reductionFunction != null && operation != null) {
      failValidation("Both Reduction Function and Op can't be assigned "
          + "for a single reduce operation.");
    }

    if (operation != null && dataType == null) {
      failValidation("Data type should specified for a reduce operation with an Op.");
    }

    if (operation != null && !Utils.dataTypeToMessageType(dataType).isPrimitive()) {
      failValidation("Reduce operations are only applicable to primitive types.");
    }
  }

  @Override
  void validate() {
    validateReduce(this.redFunction, this.op, this.getOpDataType());
  }

  public static void updateReduceEdge(Edge reduceEdge, IFunction reductionFunction,
                                      Op operation, DataType dataType) {
    if (reductionFunction != null) {
      reduceEdge.setFunction(reductionFunction);
    } else if (operation != null) {
      reduceEdge.setFunction(new ReduceFn(operation, dataType));
    }
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    updateReduceEdge(newEdge, this.redFunction, this.op, this.getOpDataType());
    return newEdge;
  }
}
