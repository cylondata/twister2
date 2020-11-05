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

import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.dl.data.TensorNumeric;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;

public class TrainReduceFunction implements ReduceFunc<DoubleDoubleArrayPair> {

  private DoubleDoubleArrayPair data;

  public TrainReduceFunction(DoubleDoubleArrayPair result) {
    this.data = result;
  }

  @Override
  public DoubleDoubleArrayPair reduce(DoubleDoubleArrayPair t1, DoubleDoubleArrayPair t2) {
    long startTime = System.nanoTime();

    this.data.setValue0(t1.getValue0() + t2.getValue0());
    double[] grad = data.getValue1();
    TensorNumeric.vAdd(grad.length, t1.getValue1(), 0, t2.getValue1(), 0, grad, 0);
    System.out.println("Iteration Reduce time : " + (System.nanoTime() - startTime) / 1e6);
    return this.data;
  }
}
