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
package edu.iu.dsc.tws.api.task.function;

import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.op.functions.reduction.ReduceOperationFunction;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IFunction;

public class ReduceFn implements IFunction {
  private static final long serialVersionUID = -123142353453456L;

  private ReduceFunction reduceFunction;

  public ReduceFn(Op op, DataType dataType) {
    reduceFunction = new ReduceOperationFunction(op, Utils.dataTypeToMessageType(dataType));
  }

  @Override
  public Object onMessage(Object object1, Object object2) {
    return reduceFunction.reduce(object1, object2);
  }
}
