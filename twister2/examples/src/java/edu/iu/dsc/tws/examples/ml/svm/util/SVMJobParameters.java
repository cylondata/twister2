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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;
import edu.iu.dsc.tws.examples.ml.svm.constant.WindowingConstants;
import edu.iu.dsc.tws.task.window.constant.WindowType;

/**
 * This class is used to define the Job Parameters needed to launch
 * a streaming or batch mode SGD based SVM
 */
public final class SVMJobParameters implements Serializable {

  private static final Logger LOG = Logger.getLogger(SVMJobParameters.class.getName());
  /**
   * Number of features in a data point
   */
  private int features;
  /**
   * Number of data points
   */
  private int samples;
  /**
   * Number of testing data points
   */
  private int testingSamples;
  private boolean isStreaming;

  /*
   * A streaming or batch mode operation
   * */
  /**
   * Training data directory {csv format data is expected {label, features}}
   */

  private String trainingDataDir;
  /**
   * Testing data directory {csv format data is expected {label, features}}
   */

  private String testingDataDir;
  /**
   * Cross-Validation data directory {csv format data is expected {label, features}}
   */

  private String crossValidationDataDir;


  /*
   * Weight vector directory {csv format data is expected {features}}
   * */

  private String weightVectorDataDir;

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
  private boolean isDummy = false;

  /*
   * Run using dummy data
   * */
  private int parallelism = 4;

  /*
   * Runs the algorithm with the given parallelism
   * */
  /**
   * Experiment name == Job Name
   */

  private String experimentName = "";

  /**
   * WindowParameter object holds the window type, window length, slide size
   */
  private WindowArguments windowArguments;


  private SVMJobParameters() {
  }

  /**
   * Builds the Job Parameters relevant SVM Algorithm
   *
   * @param cfg : this must be initialized where Job is initialized.
   */
  public static SVMJobParameters build(Config cfg) {
    SVMJobParameters svmJobParameters = new SVMJobParameters();
    svmJobParameters.features = cfg
        .getIntegerValue(MLDataObjectConstants.SgdSvmDataObjectConstants.FEATURES, 10);
    svmJobParameters.samples = cfg.getIntegerValue(
        MLDataObjectConstants.SgdSvmDataObjectConstants.SAMPLES, 1000);
    svmJobParameters.testingSamples = cfg.getIntegerValue(
        MLDataObjectConstants.SgdSvmDataObjectConstants.TESTING_SAMPLES, 1000);
    svmJobParameters.isStreaming = cfg.getBooleanValue(
        MLDataObjectConstants.STREAMING, false);
    svmJobParameters.isSplit = cfg.getBooleanValue(MLDataObjectConstants.SPLIT, false);
    svmJobParameters.trainingDataDir = cfg.getStringValue(MLDataObjectConstants.TRAINING_DATA_DIR);
    svmJobParameters.testingDataDir = cfg.getStringValue(MLDataObjectConstants.TESTING_DATA_DIR);
    svmJobParameters.weightVectorDataDir = cfg.getStringValue(MLDataObjectConstants
        .WEIGHT_VECTOR_DATA_DIR);
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
    svmJobParameters.experimentName = cfg
        .getStringValue(MLDataObjectConstants.SgdSvmDataObjectConstants.EXP_NAME);

    //set up window params
    WindowType windowType = cfg.getStringValue(WindowingConstants.WINDOW_TYPE)
        .equalsIgnoreCase("tumbling") ? WindowType.TUMBLING : WindowType.SLIDING;
    long windowLength = Long.parseLong(cfg.getStringValue(WindowingConstants.WINDOW_LENGTH));
    long slidingLength = 0;
    if (cfg.getStringValue(WindowingConstants
        .SLIDING_WINDOW_LENGTH) != null) {
      slidingLength = Long.parseLong(cfg.getStringValue(WindowingConstants
          .SLIDING_WINDOW_LENGTH));
    }

    WindowArguments windowArguments = new WindowArguments(windowType, windowLength, slidingLength,
        cfg.getBooleanValue(WindowingConstants.WINDOW_CAPACITY_TYPE));
    svmJobParameters.windowArguments = windowArguments;

    return svmJobParameters;
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

  public String getExperimentName() {
    return experimentName;
  }

  public void setExperimentName(String experimentName) {
    this.experimentName = experimentName;
  }

  public int getTestingSamples() {
    return testingSamples;
  }

  public void setTestingSamples(int testingSamples) {
    this.testingSamples = testingSamples;
  }

  public String getWeightVectorDataDir() {
    return weightVectorDataDir;
  }

  public void setWeightVectorDataDir(String weightVectorDataDir) {
    this.weightVectorDataDir = weightVectorDataDir;
  }

  public WindowArguments getWindowArguments() {
    return windowArguments;
  }

  public void setWindowArguments(WindowArguments windowArguments) {
    this.windowArguments = windowArguments;
  }

  @Override
  public String toString() {
    return "SVMJobParameters{"
        + "features=" + features
        + ", samples=" + samples
        + ", testingSamples=" + testingSamples
        + ", isStreaming=" + isStreaming
        + ", trainingDataDir='" + trainingDataDir + '\''
        + ", testingDataDir='" + testingDataDir + '\''
        + ", crossValidationDataDir='" + crossValidationDataDir + '\''
        + ", weightVecotorDataDir='" + weightVectorDataDir + '\''
        + ", modelSaveDir='" + modelSaveDir + '\''
        + ", iterations=" + iterations
        + ", alpha=" + alpha
        + ", c=" + c
        + ", isSplit=" + isSplit
        + ", isDummy=" + isDummy
        + ", parallelism=" + parallelism
        + ", experimentName=" + experimentName
        + ", windowArguments=" + windowArguments.toString()
        + '}';
  }
}
