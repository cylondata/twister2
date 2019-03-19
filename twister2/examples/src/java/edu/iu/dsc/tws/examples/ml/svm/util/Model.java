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
 * This class contains the training model
 * This can be used to provide a single object containing all the information about the
 * trained model. This class can be extended to get the behaviors of Binary classification or
 * multi-class classification model
 */
public abstract class Model {

  protected int samples;

  protected int features;

  protected double[] labels;

  protected double[] w;

  protected double alpha;

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
}
