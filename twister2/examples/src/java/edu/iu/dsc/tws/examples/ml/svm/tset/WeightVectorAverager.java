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
package edu.iu.dsc.tws.examples.ml.svm.tset;

import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;

public class WeightVectorAverager implements MapFunction<double[], double[]> {

  private int parallelism;

  public WeightVectorAverager(int parallelism) {
    this.parallelism = parallelism;
  }

  @Override
  public double[] map(double[] doubles) {
    return Matrix.scalarDivide(doubles, (double) this.parallelism);
  }
}
