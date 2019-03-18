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

import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.math.Initializer;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;
import edu.iu.dsc.tws.examples.ml.svm.sgd.StreamingSGD;


public class StreamingPegasosSGD extends StreamingSGD {

  private static int epoch = 0;
  private double[] wa;
  private int features = 0;
  private double[] xyia;

  private static final Logger LOG = Logger.getLogger(StreamingPegasosSGD.class.getName());

  public StreamingPegasosSGD(double[] x, double y, double alpha, int iterations) {
    super(x, y, alpha, iterations);
  }

  public StreamingPegasosSGD(double[] w, double[] x, double y, double alpha, int iterations) {
    super(x, y, alpha, iterations);
  }


  @Override
  public void sgd() throws NullDataSetException, MatrixMultiplicationException {
    if (isInvalid) {
      throw new NullDataSetException("Invalid data source with no features or no data");
    } else {
      // LOG.info(String.format("x.shape (%d,%d), Y.shape (%d)", x.length, 1, 1));
    }

    features = x.length;

    w = Initializer.initialWeights(features);

    double condition = y * Matrix.dot(x, w);
    if (condition < 1) {
      xyia = new double[x.length];
      xyia = Matrix.scalarMultiply(Matrix.subtract(w, Matrix.scalarMultiply(x, y)), alpha);
      w = Matrix.subtract(w, xyia);
    } else {
      wa = new double[x.length];
      wa = Matrix.scalarMultiply(w, alpha);
      w = Matrix.subtract(w, wa);
    }
    //Matrix.printVector(w);
  }
}

