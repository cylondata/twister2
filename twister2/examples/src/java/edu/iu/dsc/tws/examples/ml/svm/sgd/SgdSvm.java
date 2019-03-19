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
package edu.iu.dsc.tws.examples.ml.svm.sgd;


import java.io.Serializable;

import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;

public abstract class SgdSvm implements Serializable {

  private static final long serialVersionUID = 7674550472679161913L;

  protected double[] x;
  protected double y;
  protected double[][] xBatch;
  protected double[] yBatch;
  protected double alpha = 0.01;
  protected int features = 1;
  protected int samples = 1;
  protected boolean isInvalid = false;
  protected double[] w;
  protected int iterations = 100;
  protected long trainingTime = 0;
  protected long testingTime = 0;
  protected long dataLoadingTime = 0;

  public SgdSvm(double[] x, double y, double alpha, int iterations) {
    this.x = x;
    this.y = y;
    this.alpha = alpha;
    if (x.length == 0) {
      this.isInvalid = true;
    }

    if (x.length > 0) {
      if (x.length < 1) {
        this.isInvalid = true;
      } else {
        this.samples = 1;
        this.features = x.length;
        this.w = new double[this.features];
      }

    }
    this.iterations = iterations;

  }


  /**
   * This constructor is initialized for Batch based SGD SVM
   *
   * @param w initial weights (random Gaussian Distribution)
   * @param x data points
   * @param y labels for corresponding data points
   * @param alpha learning rate : default = 0.001
   * @param iterations number of iterations to run the SGD SVM
   */
  public SgdSvm(double[] w, double[][] x, double[] y, double alpha, int iterations) {
    this.xBatch = x;
    this.yBatch = y;
    this.alpha = alpha;
    if (x.length == 0) {
      this.isInvalid = true;
    }

    if (x.length > 0) {
      if (x.length < 1) {
        this.isInvalid = true;
      } else {
        this.samples = x.length;
        this.features = x[0].length;
        this.w = w;
      }

    }
    this.iterations = iterations;

  }

  /**
   * This constructor is initialized for Streaming based SGD SVM
   *
   * @param w initial weight
   * @param alpha learning rate : default 0.001
   * @param iterations number of iterations
   * @param features number of features in a data point
   */
  public SgdSvm(double[] w, double alpha, int iterations, int features) {
    this.alpha = alpha;
    this.w = w;
    this.iterations = iterations;
    this.features = features;
  }

  /**
   * This method is deprecated
   * @deprecated
   * Use iterativeSGD for batch mode training
   * Use onlineSGD for streaming mode training
   * @throws NullDataSetException
   * @throws MatrixMultiplicationException
   */
  @Deprecated
  public abstract void sgd() throws NullDataSetException, MatrixMultiplicationException;

  public abstract void iterativeSgd(double[] w1, double[][] x1, double[] y1)
      throws NullDataSetException, MatrixMultiplicationException;

  public abstract void onlineSGD(double[] w1, double[] x1, double y1)
      throws NullDataSetException, MatrixMultiplicationException;

  public double[] getW() {
    return w;
  }

  public void setW(double[] w) {
    this.w = w;
  }
}
