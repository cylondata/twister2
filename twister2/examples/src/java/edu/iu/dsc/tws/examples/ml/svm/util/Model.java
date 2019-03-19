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

import java.io.Serializable;

/**
 * This class contains the training model
 * This can be used to provide a single object containing all the information about the
 * trained model. This class can be extended to get the behaviors of Binary classification or
 * multi-class classification model
 */
public abstract class Model implements Serializable {

  private static final long serialVersionUID = -3452275060295386696L;

  /**
   * Number of data points
   */
  protected int samples;

  /**
   * Number of features in a data point (dimensions)
   */
  protected int features;

  /**
   * Class labels in the classification {+1,-1} is the default for Binary Classification
   */
  protected double[] labels;

  /**
   * Weight Vector
   */
  protected double[] w;

  /**
   * Learning rate
   */
  protected double alpha;

  public Model() {
  }

  public Model(int samples, int features, double[] labels, double[] w) {
    this.samples = samples;
    this.features = features;
    this.labels = labels;
    this.w = w;
  }

  public Model(int samples, int features, double[] labels, double[] w, double alpha) {
    this.samples = samples;
    this.features = features;
    this.labels = labels;
    this.w = w;
    this.alpha = alpha;
  }

  public int getSamples() {
    return samples;
  }

  public int getFeatures() {
    return features;
  }

  public double[] getLabels() {
    return labels;
  }

  public double[] getW() {
    return w;
  }

  public double getAlpha() {
    return alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public abstract void saveModel(String file);
}
