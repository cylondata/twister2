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
package edu.iu.dsc.tws.examples.ml.svm.aggregate;


public class IterativeTrainingReduce implements ISvmIterativeReduceFunction<double[][]> {
  @Override
  public double[][] onMessage(double[][] object1, double[][] object2) {
    double[][] res = new double[1][object1[0].length];
    for (int i = 0; i < res[0].length; i++) {
      res[0][i] = object1[0][i] + object2[0][i];
    }
    return res;
  }
}
