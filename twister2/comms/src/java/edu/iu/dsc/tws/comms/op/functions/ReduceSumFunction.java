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
package edu.iu.dsc.tws.comms.op.functions;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;

/**
 * This class is the Sum function for reduce operations. It will simply sum the
 * two values given in the two objects and return the sum.
 */
public class ReduceSumFunction implements ReduceFunction {
  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  @Override
  public Object reduce(Object t1, Object t2) {
    if (t1 instanceof int[] && t2 instanceof int[]) {
      int[] t1Array = (int[]) t1;
      int[] t2Array = (int[]) t2;
      int[] results = new int[t1Array.length];
      for (int i = 0; i < t1Array.length; i++) {
        results[i] = t1Array[i] + t2Array[i];
      }
      return results;
    } else {
      throw new RuntimeException("Operation not supported");
    }
  }
}
