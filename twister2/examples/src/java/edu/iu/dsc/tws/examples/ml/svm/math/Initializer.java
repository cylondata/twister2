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
package edu.iu.dsc.tws.examples.ml.svm.math;

import java.util.Random;

public final class Initializer {

  private Initializer() {
  }

  public static double[] initialWeights(int size) {
    double[] initW = new double[size];
    Random random = new Random();
    for (int i = 0; i < size; i++) {
      initW[i] = random.nextGaussian();
    }

    return initW;
  }

  public static double[] initZeros(int size) {
    double[] initW = new double[size];
    Random random = new Random();
    for (int i = 0; i < size; i++) {
      initW[i] = 0;
    }

    return initW;
  }
}
