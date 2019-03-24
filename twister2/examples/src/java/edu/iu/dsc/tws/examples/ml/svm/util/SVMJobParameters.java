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

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;

/**
 * This class is used to define the Job Parameters needed to launch
 * a streaming or batch mode SGD based SVM
 */
public final class SVMJobParameters {

  private static final Logger LOG = Logger.getLogger(SVMJobParameters.class.getName());

  private SVMJobParameters() { }

  /**
   * Number of features in a data point
   */
  private int features;

  /**
   * Number of data points
   * */
  private int samples;

  /*
  * A streaming or batch mode operation
  * */

  private boolean isStreaming;

  /**
   * Training data directory {csv format data is expected {label, features}}
   * */

  private String trainingDataDir;

  /**
   * Testing data directory {csv format data is expected {label, features}}
   * */

  private String testingDataDir;

  /**
   * Cross-Validation data directory {csv format data is expected {label, features}}
   * */

  private String crossValidationDataDir;

  /**
   * Saving trained model
   */
  private String modelSaveDir;

  /**
   * Iterations in training model
   * Stream mode , iterations = 1
   */
  private int iterations = 100;

  /**
   * Learning rate
   */
  private double alpha = 0.001;


  /**
   * Hyper Parameter ; default is 1.0
   */
  private double c = 1.0;

  /**
   * Is data split from a single source [training, cross-validation, testing]
   */
  private boolean isSplit = false;

  /*
  * Run using dummy data
  * */

  private boolean isDummy = false;

  /*
  * Runs the algorithm with the given parallelism
  * */

  private int parallelism = 4;

  /**
   * Builds the Job Parameters relevant SVM Algorithm
   * @param cfg : this must be initialized where Job is initialized.
   * @return
   */
  public static SVMJobParameters build(Config cfg) {
    SVMJobParameters svmJobParameters = new SVMJobParameters();
    svmJobParameters.features = cfg
        .getIntegerValue(MLDataObjectConstants.SgdSvmDataObjectConstants.FEATURES, 10);
    svmJobParameters.samples = cfg.getIntegerValue(
        MLDataObjectConstants.SgdSvmDataObjectConstants.SAMPLES, 1000);
    svmJobParameters.isStreaming = cfg.getBooleanValue(
        MLDataObjectConstants.STREAMING, false);
    svmJobParameters.isSplit = cfg.getBooleanValue(MLDataObjectConstants.SPLIT, false);
    svmJobParameters.trainingDataDir = cfg.getStringValue(MLDataObjectConstants.TRAINING_DATA_DIR);
    svmJobParameters.testingDataDir = cfg.getStringValue(MLDataObjectConstants.TESTING_DATA_DIR);
    svmJobParameters.crossValidationDataDir = cfg
        .getStringValue(MLDataObjectConstants.CROSS_VALIDATION_DATA_DIR);
    svmJobParameters.modelSaveDir = cfg
        .getStringValue(MLDataObjectConstants.MODEL_SAVE_PATH);
    svmJobParameters.iterations = cfg
        .getIntegerValue(MLDataObjectConstants.SgdSvmDataObjectConstants.ITERATIONS,
            100);
    svmJobParameters.c = cfg.getDoubleValue(MLDataObjectConstants.SgdSvmDataObjectConstants.C,
        1.0);
    svmJobParameters.alpha = cfg
        .getDoubleValue(MLDataObjectConstants.SgdSvmDataObjectConstants.ALPHA, 0.001);
    svmJobParameters.isDummy = cfg.getBooleanValue(
        MLDataObjectConstants.DUMMY, true);
    svmJobParameters.parallelism = cfg
        .getIntegerValue(WorkerConstants.PARALLELISM, 4);

    return  svmJobParameters;
  }

  public int getFeatures() {
    return features;
  }

  public void setFeatures(int features) {
    this.features = features;
  }

  public int getSamples() {
    return samples;
  }

  public void setSamples(int samples) {
    this.samples = samples;
  }

  public boolean isStreaming() {
    return isStreaming;
  }

  public void setStreaming(boolean streaming) {
    isStreaming = streaming;
  }

  public String getTrainingDataDir() {
    return trainingDataDir;
  }

  public void setTrainingDataDir(String trainingDataDir) {
    this.trainingDataDir = trainingDataDir;
  }

  public String getTestingDataDir() {
    return testingDataDir;
  }

  public void setTestingDataDir(String testingDataDir) {
    this.testingDataDir = testingDataDir;
  }

  public String getCrossValidationDataDir() {
    return crossValidationDataDir;
  }

  public void setCrossValidationDataDir(String crossValidationDataDir) {
    this.crossValidationDataDir = crossValidationDataDir;
  }

  public String getModelSaveDir() {
    return modelSaveDir;
  }

  public void setModelSaveDir(String modelSaveDir) {
    this.modelSaveDir = modelSaveDir;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }

  public double getAlpha() {
    return alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public double getC() {
    return c;
  }

  public void setC(double c) {
    this.c = c;
  }

  public boolean isSplit() {
    return isSplit;
  }

  public void setSplit(boolean split) {
    isSplit = split;
  }

  public boolean isDummy() {
    return isDummy;
  }

  public void setDummy(boolean dummy) {
    isDummy = dummy;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  @Override
  public String toString() {
    return "SVMJobParameters{"
        + "features=" + features
        + ", samples=" + samples
        + ", isStreaming=" + isStreaming
        + ", trainingDataDir='" + trainingDataDir + '\''
        + ", testingDataDir='" + testingDataDir + '\''
        + ", crossValidationDataDir='" + crossValidationDataDir + '\''
        + ", modelSaveDir='" + modelSaveDir + '\''
        + ", iterations=" + iterations
        + ", alpha=" + alpha
        + ", c=" + c
        + ", isSplit=" + isSplit
        + ", isDummy=" + isDummy
        + ", parallelism=" + parallelism
        + '}';
  }
}
