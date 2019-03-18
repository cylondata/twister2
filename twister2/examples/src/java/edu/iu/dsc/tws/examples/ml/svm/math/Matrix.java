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

import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;


public final class Matrix {

  private Matrix() {
  }

  private static final Logger LOG = Logger.getLogger(Matrix.class.getName());

  public static double[] scalarMultiply(double[] x, double y) {
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = x[i] * y;
    }
    return result;
  }

  public static double[] scalarDivide(double[] x, double y) {
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = x[i] / y;
    }
    return result;
  }

  public static double[] scalarAddition(double[] x, double y) {
    double[] result = new double[x.length];
    for (int i = 0; i < x.length; i++) {
      result[i] = x[i] + y;
    }
    return result;
  }

  public static double[] multiply(double[] x, double[] w)
      throws MatrixMultiplicationException {
    if (x.length == w.length) {
      double[] result = new double[x.length];
      for (int i = 0; i < x.length; i++) {
        result[i] = x[i] * w[i];
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + x.length + ", w.length : " + w.length);
    }
  }

  public static double[] divide(double[] x, double[] w) throws MatrixMultiplicationException {
    if (x.length == w.length) {
      double[] result = new double[x.length];
      for (int i = 0; i < x.length; i++) {
        result[i] = x[i] / w[i];
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + x.length + ", w.length : " + w.length);
    }
  }

  public static double[] sqrt(double[] x) throws MatrixMultiplicationException {
    if (x.length > 0) {
      double[] result = new double[x.length];
      for (int i = 0; i < x.length; i++) {
        result[i] = Math.sqrt(x[i]);
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + x.length);
    }
  }

  public static double[] add(double[] w1, double[] w2) throws MatrixMultiplicationException {
    if (w1.length == w2.length) {
      double[] result = new double[w1.length];
      for (int i = 0; i < w1.length; i++) {
        result[i] = w1[i] + w2[i];
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + w1.length + ", w.length : " + w2.length);
    }
  }

  public static double[] subtract(double[] w1, double[] w2)
      throws MatrixMultiplicationException {
    if (w1.length == w2.length) {
      double[] result = new double[w1.length];
      for (int i = 0; i < w1.length; i++) {
        result[i] = w1[i] - w2[i];
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + w1.length + ", w.length : " + w2.length);
    }
  }

  public static double dot(double[] x, double[] w) throws MatrixMultiplicationException {
    if (x.length == w.length) {
      double result = 0;
      for (int i = 0; i < x.length; i++) {
        result += x[i] * w[i];
      }
      return result;
    } else {
      throw new MatrixMultiplicationException("Invalid Dimensions x.length "
          + x.length + ", w.length : " + w.length);
    }
  }


  public static void printVector(double[] mat) {
    String s = "";
    for (int i = 0; i < mat.length; i++) {
      s += mat[i] + " ";
    }
    LOG.info("Print Matrix : " + s);
  }

  public static void printMatrix(double[][] mat) {
    for (int i = 0; i < mat.length; i++) {
      for (int j = 0; j < mat[0].length; j++) {
        System.out.print(mat[i][j] + " ");
      }
      System.out.println();
    }
    System.out.println();
  }
}
