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
package edu.iu.dsc.tws.examples.ml.svm.util;

import java.util.Random;
import java.util.logging.Logger;

public final class DataGenerator {

  private static final Logger LOG = Logger.getLogger(DataGenerator.class.getName());

  private DataGenerator() {
  }

  /**
   * This method populates the array with a Gaussian Distribution
   *
   * @param features : number of features in a data point
   */
  public static double[] seedDoubleArray(int features) {
    double[] ar = new double[features];
    Random r = new Random();
    for (int i = 0; i < features; i++) {
      ar[i] = r.nextGaussian();
    }
    return ar;
  }

}
