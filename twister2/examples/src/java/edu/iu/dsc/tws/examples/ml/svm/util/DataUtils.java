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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;

public final class DataUtils {

  private static final Logger LOG = Logger.getLogger(DataUtils.class.getName());

  private static final double[] LABELS = {-1, +1};

  private static final boolean DEBUG = false;

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


  /**
   * This method is deprecated and use the updateModelData method to update the model with
   * data points
   *
   * @param xy data points with {y_i, x_i_1, .... x_i_d}
   * @param iterations number of iterations
   * @param alpha learning rate
   * @return returns the BinaryBatchModel
   * @deprecated method
   */
  @Deprecated
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

  /**
   * This method updates an existing BinaryBatchModel with the data points
   *
   * @param binaryBatchModel Binary Batch Model
   * @param xy data points with {y_i, x_i_1, .... x_i_d}
   * @return returns the updated model
   */
  public static BinaryBatchModel updateModelData(BinaryBatchModel binaryBatchModel, double[][] xy) {
    if (binaryBatchModel == null) {
      throw new NullPointerException("BinaryBatchModel is null !!!");
    } else {
      double[] w = binaryBatchModel.getW();
      if (DEBUG) {
        LOG.info(String.format("W : %s ", Arrays.toString(w)));
      }

      int features = binaryBatchModel.getFeatures();
      int samples = binaryBatchModel.getSamples();
      if (xy.length > 0 && features > 0) {
        if (xy[0].length > 0 && samples > 0) {
          double[][] x = new double[xy.length][xy[0].length];
          double[] y = new double[xy.length];
          for (int i = 0; i < xy.length; i++) {
            y[i] = xy[i][0];
            x[i] = Arrays.copyOfRange(xy[i], 1, xy[0].length);
          }
          binaryBatchModel.setX(x);
          binaryBatchModel.setY(y);
          binaryBatchModel.setW(w);
          binaryBatchModel.setSamples(samples);
          binaryBatchModel.setFeatures(features);
          if (DEBUG) {
            LOG.info(String.format("X : %s, y : %s, Samples %d",
                Arrays.toString(binaryBatchModel.getX()[0]),
                binaryBatchModel.getY()[0], binaryBatchModel.getY().length));
          }

        }
      } else {
        LOG.severe(String.format("Something Went Wrong"));
      }
    }
    return binaryBatchModel;
  }

  /**
   * This method is used to convert the input data obtained from a different SourceTask
   */
  public static double[][] getDataPointsFromDataObject(Object object) {
    double[][] res = null;
    if (object instanceof ArrayList<?>) {
      ArrayList<?> data = (ArrayList<?>) object;
      res = new double[data.size()][];
      int count = 0;
      for (Object o : data) {
        if (o instanceof String) {
          //LOG.info(String.format("Transformed Data : %s", (String) o));
          //each string contains the a , delimeter string
          // the first value is the class label and the rest are features of the datapoint
          String[] s = String.valueOf(o).split(Constants.SimpleGraphConfig.DELIMITER);
          double label = Double.parseDouble(s[0]);
          int features = s.length - 1;
          res[count] = new double[s.length];
          for (int i = 0; i < s.length; i++) {
            res[count][i] = Double.parseDouble(s[i]);
          }
//          for (int i = 0; i < features; i++) {
//            res[count][i] = Double.parseDouble(s[i + 1]);
//          }
          count++;
        } else {
          LOG.info(String.format("Data Type : %s", o.getClass().getName()));
        }
      }
    }
    return res;
  }

  public static double[] getWeightVectorFromWeightVectorObject(Object object) {
    double[] weightVector = null;

    return weightVector;
  }

  public static double[][] getDataObjectToDoubleArray(DataObject<Object> dataPointsObject1) {
    double[][] d = null;
    LOG.info(String.format("Which Type %s", dataPointsObject1.getClass().getName()));
    LOG.info(String.format("How Much %d", dataPointsObject1.getPartitions().length));
    LOG.info(String.format("Next Element %s",
        dataPointsObject1.getPartitions()[0].getConsumer().next().getClass().getName()));
    return d;
  }


}
