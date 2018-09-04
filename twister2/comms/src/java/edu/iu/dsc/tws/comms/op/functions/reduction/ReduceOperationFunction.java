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
package edu.iu.dsc.tws.comms.op.functions.reduction;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.data.api.DataType;

public class ReduceOperationFunction implements ReduceFunction {

  private DataType dataType;
  private Op operation;

  public ReduceOperationFunction(Op operation, DataType dtype) {
    this.operation = operation;
    this.dataType = dtype;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  @Override
  public Object reduce(Object t1, Object t2) {
    Object result = null;
    if (this.dataType == DataType.INTEGER) {
      if (t1 instanceof int[] && t2 instanceof int[]) {
        int[] i1 = (int[]) t1;
        int[] i2 = (int[]) t2;
        int[] res = new int[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = i1[i] + i2[i];
        }
        result = res;
      }
    }
    return result;
  }
}
