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

/**
 * This class is an extended Model which is specialized to hold the parameters
 * involved in the Binary Batch based training
 */
public class BinaryBatchModel extends Model {

  private double[][] x;

  private double[] y;

  private double[][] xy;

  private int iterations;

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w) {
    super(samples, features, labels, w);
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w, double[][] x) {
    super(samples, features, labels, w);
    this.x = x;
    this.y = labels;
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w,
                          double alpha, double[][] x) {
    super(samples, features, labels, w, alpha);
    this.x = x;
    this.y = labels;
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w,
                          double alpha, double[][] x, int iterations) {
    super(samples, features, labels, w, alpha);
    this.x = x;
    this.y = labels;
    this.iterations = iterations;
  }

  @Override
  public int getSamples() {
    return super.getSamples();
  }

  @Override
  public int getFeatures() {
    return super.getFeatures();
  }

  @Override
  public double[] getLabels() {
    return super.getLabels();
  }

  @Override
  public double[] getW() {
    return super.getW();
  }

  public double[][] getX() {
    return x;
  }

  public void setX(double[][] x) {
    this.x = x;
  }

  public double[] getY() {
    return y;
  }

  public void setY(double[] y) {
    this.y = y;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }
}
