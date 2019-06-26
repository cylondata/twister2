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
package edu.iu.dsc.tws.task.impl.function;

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.comms.functions.reduction.ReduceOperationFunction;

/**
 * The reduce function wrapping the operation and data type.
 */
public class ReduceFn implements IFunction {
  private static final long serialVersionUID = -123142353453456L;

  /**
   * The actual reduce function
   */
  private ReduceFunction reduceFunction;

  public ReduceFn(Op op, MessageType dataType) {
    reduceFunction = new ReduceOperationFunction(op, dataType);
  }

  @Override
  public Object onMessage(Object object1, Object object2) {
    return reduceFunction.reduce(object1, object2);
  }
}
