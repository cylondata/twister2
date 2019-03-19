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

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

public final class DataUtils {

  private static final Logger LOG = Logger.getLogger(DataUtils.class.getName());

  private static final double[] LABELS = {-1, +1};

  private DataUtils() {
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

  /**
   * This method combines the label and features in the data point
   * This can be use to submit a single message in the stream
   *
   * @param x data points with d features
   * @param y label
   * @return [y, x_i, ...x_d]
   */
  public static double[] combineLabelAndData(double[] x, double y) {
    double[] res = new double[x.length + 1];
    res[0] = y;
    for (int i = 1; i < res.length; i++) {
      res[i] = x[i - 1];
    }
    return res;
  }

  /**
   * This method provides a dummy data set for batch based computations
   * User can say the number of samples and feature size and this function
   * generates a data set with labels included per data sample
   *
   * @param samples number of data points = N
   * @param features number of features in a data point = D
   * @return data.length = N, data[0].length = D + 1, i.e: data[i][0]== label, data[i][1:D+1]== {x}
   */
  public static double[][] generateDummyDataPoints(int samples, int features) {
    double[][] data = new double[samples][features + 1];
    Random random = new Random();
    for (int i = 0; i < samples; i++) {
      data[i][0] = LABELS[random.nextInt(LABELS.length)];
      for (int j = 1; j < features + 1; j++) {
        data[i][j] = random.nextGaussian();
      }
    }
    return data;
  }

  public static BinaryBatchModel generateBinaryModel(double[][] xy, int iterations, double alpha) {
    BinaryBatchModel binaryBatchModel = null;

    if (xy.length > 0) {
      if (xy[0].length > 0) {
        int features = xy[0].length - 1;
        int samples = xy.length;
        double[] w = seedDoubleArray(features);
        double[][] x = new double[samples][features];
        double[] y = new double[samples];
        for (int i = 0; i < samples; i++) {
          y[i] = xy[i][0];
          x[i] = Arrays.copyOfRange(xy[i], 1, features + 1);
        }
        binaryBatchModel = new BinaryBatchModel(samples, features, y, w, x);
      }
    }

    return binaryBatchModel;
  }

}
