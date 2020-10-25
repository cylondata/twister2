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
package edu.iu.dsc.tws.dl.optim;

import edu.iu.dsc.tws.api.tset.fn.BaseMapFunc;
import edu.iu.dsc.tws.dl.data.TensorNumeric;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;

public class AverageParameters extends BaseMapFunc<DoubleDoubleArrayPair, DoubleDoubleArrayPair> {

  @Override
  public DoubleDoubleArrayPair map(DoubleDoubleArrayPair input) {
    int parallelism = this.getTSetContext().getParallelism();
    input.setValue0(input.getValue0() / parallelism);
    double[] data = input.getValue1();
    TensorNumeric.scal(data.length, 1.0 / parallelism, data, 0, 1);
    return input;
  }
}
