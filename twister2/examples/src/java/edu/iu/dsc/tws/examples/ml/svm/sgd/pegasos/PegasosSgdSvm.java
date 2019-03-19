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
package edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos;

import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.math.Initializer;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;
import edu.iu.dsc.tws.examples.ml.svm.sgd.SgdSvm;


public class PegasosSgdSvm extends SgdSvm implements Serializable {

  private static final long serialVersionUID = -8279454451787246995L;

  private static int epoch = 0;
  private double[] wa;
  private int features = 0;
  private double[] xyia;

  private static final Logger LOG = Logger.getLogger(PegasosSgdSvm.class.getName());

  public PegasosSgdSvm(double[] x, double y, double alpha, int iterations) {
    super(x, y, alpha, iterations);
  }

  public PegasosSgdSvm(double[] w, double[] x, double y, double alpha, int iterations) {
    super(x, y, alpha, iterations);
  }

  public PegasosSgdSvm(double[] w, double alpha, int iterations, int features) {
    super(w, alpha, iterations, features);
    this.features = features;
    this.w = Initializer.initialWeights(this.features);
  }

  public PegasosSgdSvm(double[] w, double[][] x, double[] y, double alpha, int iterations,
                       int features) {
    super(w, x, y, alpha, iterations);
    this.features = features;
    if (w == null) {
      this.w = Initializer.initialWeights(this.features);
    } else {
      this.w = w;
    }

  }

  /**
   * This method is deprected
   * @deprecated
   * Use iterativeSGD for batch mode training
   * Use onlineSGD for streaming mode training
   * @throws NullDataSetException
   * @throws MatrixMultiplicationException
   */
  @Override
  @Deprecated
  public void sgd() throws NullDataSetException, MatrixMultiplicationException {
    if (isInvalid) {
      throw new NullDataSetException("Invalid data source with no features or no data");
    } else {
      // LOG.info(String.format("x.shape (%d,%d), Y.shape (%d)", x.length, 1, 1));
    }
  }

  @Override
  public void iterativeSgd(double[] w, double[][] x, double[] y)
      throws NullDataSetException, MatrixMultiplicationException {
    double[] currentW = w;
    for (int i = 0; i < this.iterations; i++) {
      for (int j = 0; j < x.length; j++) {
        double condition = y[i] * Matrix.dot(x[j], currentW);
        double[] newW;
        if (condition < 1) {
          this.xyia = new double[x.length];
          this.xyia = Matrix.scalarMultiply(Matrix
              .subtract(currentW, Matrix.scalarMultiply(x[j], y[i])), alpha);
          newW = Matrix.subtract(currentW, xyia);
        } else {
          wa = new double[x.length];
          wa = Matrix.scalarMultiply(currentW, alpha);
          newW = Matrix.subtract(currentW, wa);
        }
        currentW = newW;
      }
    }
    this.setW(currentW);
  }

  @Override
  public void onlineSGD(double[] w, double[] x, double y)
      throws NullDataSetException, MatrixMultiplicationException {
    double condition = y * Matrix.dot(x, w);
    double[] newW;
    if (condition < 1) {
      this.xyia = new double[x.length];
      this.xyia = Matrix.scalarMultiply(Matrix.subtract(w, Matrix.scalarMultiply(x, y)), alpha);
      newW = Matrix.subtract(w, xyia);
    } else {
      wa = new double[x.length];
      wa = Matrix.scalarMultiply(w, alpha);
      newW = Matrix.subtract(w, wa);
    }
    this.setW(newW);
  }

  @Override
  public double[] getW() {
    return this.w;
  }

  @Override
  public void setW(double[] w) {
    this.w = w;
  }
}

