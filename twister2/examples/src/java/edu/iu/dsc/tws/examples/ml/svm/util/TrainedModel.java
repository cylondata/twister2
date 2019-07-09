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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

public class TrainedModel extends Model {
  private static final long serialVersionUID = -6049759570540858599L;

  private static final Logger LOG = Logger.getLogger(TrainedModel.class.getName());

  private double accuracy;

  private String modelName;

  public TrainedModel() {
  }

  public TrainedModel(int samples, int features, double[] labels, double[] w) {
    super(samples, features, labels, w);
  }

  public TrainedModel(int samples, int features, double[] labels, double[] w, double alpha) {
    super(samples, features, labels, w, alpha);
  }

  public TrainedModel(double accuracy, String modelname) {
    this.accuracy = accuracy;
    this.modelName = modelname;
  }

  public TrainedModel(int samples, int features, double[] labels, double[] w, double accuracy,
                      String modelname) {
    super(samples, features, labels, w);
    this.accuracy = accuracy;
    this.modelName = modelname;
  }

  public TrainedModel(int samples, int features, double[] labels, double[] w, double alpha,
                      double accuracy, String modelname) {
    super(samples, features, labels, w, alpha);
    this.accuracy = accuracy;
    this.modelName = modelname;
  }

  public TrainedModel(BinaryBatchModel binaryBatchModel, double accuracy, String modelname) {
    this.samples = binaryBatchModel.getSamples();
    this.features = binaryBatchModel.getFeatures();
    this.labels = binaryBatchModel.getLabels();
    this.w = binaryBatchModel.getW();
    this.alpha = binaryBatchModel.getAlpha();
    this.accuracy = accuracy;
    this.modelName = modelname;
  }

  @Override
  public void saveModel(String file) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(file));
      fileWriter.write(this.toString());
      fileWriter.write("/n");
    } catch (IOException e) {
      LOG.severe(String.format("IO Exception : %s", e.getMessage()));
    } finally {
      try {
        fileWriter.close();
      } catch (IOException e) {
        LOG.severe(String.format("IO Exception : %s", e.getMessage()));
      }
    }
  }

  public double getAccuracy() {
    return accuracy;
  }

  public void setAccuracy(double accuracy) {
    this.accuracy = accuracy;
  }

  public String getModelName() {
    return modelName;
  }

  public void setModelName(String modelName) {
    this.modelName = modelName;
  }

  @Override
  public String toString() {
    return "TrainedModel{"
        + "accuracy=" + accuracy
        + ", modelName='" + modelName + '\''
        + ", samples=" + samples
        + ", features=" + features
        + ", labels=" + Arrays.toString(labels)
        + ", w=" + Arrays.toString(w)
        + ", alpha=" + alpha
        + '}';
  }
}
