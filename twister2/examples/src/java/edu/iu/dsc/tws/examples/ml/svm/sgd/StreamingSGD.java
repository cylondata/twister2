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


import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;

public abstract class StreamingSGD {

  protected double[] x;
  protected double y;
  protected double alpha = 0.01;
  protected int features = 1;
  protected int samples = 1;
  protected boolean isInvalid = false;
  protected double[] w;
  protected int iterations = 100;
  protected long trainingTime = 0;
  protected long testingTime = 0;
  protected long dataLoadingTime = 0;

  public StreamingSGD(double[] x, double y, double alpha, int iterations) {
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

  public StreamingSGD(double[] w, double[] x, double y, double alpha, int iterations) {
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
        this.w = w;
      }

    }
    this.iterations = iterations;

  }

  public abstract void sgd() throws NullDataSetException, MatrixMultiplicationException;

  public double[] getW() {
    return w;
  }

  public void setW(double[] w) {
    this.w = w;
  }
}
