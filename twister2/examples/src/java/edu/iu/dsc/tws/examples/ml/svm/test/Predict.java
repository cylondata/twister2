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
package edu.iu.dsc.tws.examples.ml.svm.test;


import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;

public class Predict {
  private double[][] xtest;
  private double[] ytest;
  private double[] w;
  private double accuracy = 0.0;
  private int correctCount = 0;

  public Predict(double[][] xtest, double[] ytest, double[] w) {
    this.xtest = xtest;
    this.ytest = ytest;
    this.w = w;
  }

  public double predict() throws MatrixMultiplicationException {

    for (int i = 0; i < xtest.length; i++) {
      double pred = 1;
      double d = Matrix.dot(xtest[i], w);
      pred = Math.signum(d);
      if (ytest[i] == pred) {
        correctCount++;
      }
      //System.out.println(pred+"/"+ytest[i]+ " ==> " + d);
    }
    accuracy = ((double) correctCount / (double) xtest.length) * 100.0;

    return accuracy;
  }


  public double predict(double[] w1) throws MatrixMultiplicationException {

    for (int i = 0; i < xtest.length; i++) {
      double pred = 1;
      double d = Matrix.dot(xtest[i], w1);
      pred = Math.signum(d);
      if (ytest[i] == pred) {
        correctCount++;
      }
      //System.out.println(pred+"/"+ytest[i]+ " ==> " + d);
    }
    accuracy = ((double) correctCount / (double) xtest.length) * 100.0;
    correctCount = 0;
    return accuracy;
  }
}
