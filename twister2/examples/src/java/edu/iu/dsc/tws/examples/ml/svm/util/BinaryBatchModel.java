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

/**
 * This class is an extended Model which is specialized to hold the parameters
 * involved in the Binary Batch based training
 */
public class BinaryBatchModel extends Model {

  private static final long serialVersionUID = 4109749989421999985L;

  private double[][] x;

  private double[] y;

  private double[][] xy;

  private int iterations;

  private int features;

  private int samples;

  private double alpha;

  private double[] w;

  public BinaryBatchModel() {
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w) {
    super(samples, features, labels, w);
    this.samples = samples;
    this.features = features;
    this.labels = labels;
    this.w = w;
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w, double[][] x) {
    super(samples, features, labels, w);
    this.x = x;
    this.y = labels;
    this.features = features;
    this.w = w;
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w,
                          double alpha, double[][] x) {
    super(samples, features, labels, w, alpha);
    this.x = x;
    this.y = labels;
    this.features = features;
    this.alpha = alpha;
  }

  public BinaryBatchModel(int samples, int features, double[] labels, double[] w,
                          double alpha, double[][] x, int iterations) {
    super(samples, features, labels, w, alpha);
    this.x = x;
    this.y = labels;
    this.iterations = iterations;
    this.features = features;
    this.samples = samples;
    this.alpha = alpha;
  }


  @Override
  public int getSamples() {
    return  this.samples;
  }

  @Override
  public int getFeatures() {
    return this.features;
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

  public void setFeatures(int features) {
    this.features = features;
  }

  public void setSamples(int samples) {
    this.samples = samples;
  }

  @Override
  public double getAlpha() {
    return alpha;
  }

  @Override
  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public void setW(double[] w) {
    this.w = w;
  }

  @Override
  public void saveModel(String file) {
    // save model
  }

  @Override
  public String toString() {
    return "BinaryBatchModel{"
        + "iterations=" + iterations
        + ", features=" + features
        + ", samples=" + samples
        + ", alpha=" + alpha
        + ", w=" + Arrays.toString(w)
        + '}';
  }
}
