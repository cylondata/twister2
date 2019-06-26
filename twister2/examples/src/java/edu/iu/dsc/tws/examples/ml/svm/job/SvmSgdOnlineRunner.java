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
package edu.iu.dsc.tws.examples.ml.svm.job;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMAccuracyReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMWeightVectorReduce;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.constant.TimingConstants;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeDataStream;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativePredictionDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TGUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SvmSgdOnlineRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdOnlineRunner.class.getName());

  private static final String DELIMITER = ",";
  private int dataStreamerParallelism = 4;
  private int svmComputeParallelism = 4;
  private int features = 10;
  private OperationMode operationMode;
  private SVMJobParameters svmJobParameters;
  private BinaryBatchModel binaryBatchModel;
  private TaskGraphBuilder trainingBuilder;
  private TaskGraphBuilder testingBuilder;
  private DataFlowTaskGraph iterativeSVMTrainingTaskGraph;
  private ExecutionPlan iterativeSVMTrainingExecutionPlan;
  private DataFlowTaskGraph iterativeSVMTestingTaskGraph;
  private ExecutionPlan iterativeSVMTestingExecutionPlan;
  private DataFlowTaskGraph weightVectorTaskGraph;
  private ExecutionPlan weightVectorExecutionPlan;
  private IterativeDataStream iterativeDataStream;
  private IterativePredictionDataStreamer iterativePredictionDataStreamer;
  private IterativeSVMAccuracyReduce iterativeSVMAccuracyReduce;
  private IterativeSVMWeightVectorReduce iterativeSVMRiterativeSVMWeightVectorReduce;
  private DataObject<double[][]> trainingDoubleDataPointObject;
  private DataObject<double[][]> testingDoubleDataPointObject;
  private DataObject<double[]> inputDoubleWeightvectorObject;
  private DataObject<Double> finalAccuracyDoubleObject;
  private long initializingTime = 0L;
  private long dataLoadingTime = 0L;
  private long trainingTime = 0L;
  private long testingTime = 0L;
  private double initializingDTime = 0L;
  private double dataLoadingDTime = 0L;
  private double trainingDTime = 0L;
  private double testingDTime = 0L;
  private double totalDTime = 0L;
  private double accuracy = 0L;
  private boolean debug = false;
  private String experimentName = "";

  @Override
  public void execute() {
    // method 2
    this.initialize()
        .loadData()
        .summary();
  }

  /**
   * This method initializes the parameters in running SVM
   */
  public void initializeParameters() {
    this.svmJobParameters = SVMJobParameters.build(config);
    this.binaryBatchModel = new BinaryBatchModel();
    this.dataStreamerParallelism = this.svmJobParameters.getParallelism();
    this.experimentName = this.svmJobParameters.getExperimentName();
    // svm compute parallelism can be set as a configurable parameter
    this.svmComputeParallelism = this.dataStreamerParallelism;
    this.features = this.svmJobParameters.getFeatures();
    this.binaryBatchModel.setIterations(this.svmJobParameters.getIterations());
    this.binaryBatchModel.setAlpha(this.svmJobParameters.getAlpha());
    this.binaryBatchModel.setFeatures(this.svmJobParameters.getFeatures());
    this.binaryBatchModel.setSamples(this.svmJobParameters.getSamples());
    this.binaryBatchModel.setW(DataUtils.seedDoubleArray(this.svmJobParameters.getFeatures()));
    LOG.info(this.binaryBatchModel.toString());
    this.operationMode = this.svmJobParameters.isStreaming()
        ? OperationMode.STREAMING : OperationMode.BATCH;
    trainingBuilder = TaskGraphBuilder.newBuilder(config);
    testingBuilder = TaskGraphBuilder.newBuilder(config);
  }

  public SvmSgdOnlineRunner initialize() {
    long t1 = System.nanoTime();
    initializeParameters();
    this.initializingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdOnlineRunner loadData() {
    long t1 = System.nanoTime();
    loadTrainingData();
    loadTestingData();
    this.dataLoadingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdOnlineRunner withWeightVector() {
    long t1 = System.nanoTime();
    loadWeightVector();
    this.dataLoadingTime += System.nanoTime() - t1;
    return this;
  }

  public SvmSgdOnlineRunner summary() {
    generateSummary();
    return this;
  }


  private void loadTrainingData() {
    DataFlowTaskGraph trainingDFTG = TGUtils.buildTrainingDataPointsTG(this.dataStreamerParallelism,
        this.svmJobParameters, this.config, this.operationMode);
    ExecutionPlan trainingEP = taskExecutor.plan(trainingDFTG);
    taskExecutor.execute(trainingDFTG, trainingEP);
    trainingDoubleDataPointObject = taskExecutor
        .getOutput(trainingDFTG, trainingEP,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK);
    for (int i = 0; i < trainingDoubleDataPointObject.getPartitions().length; i++) {
      double[][] datapoints = trainingDoubleDataPointObject.getPartitions()[i].getConsumer()
          .next();
      LOG.info(String.format("Training Datapoints : %d,%d", datapoints.length, datapoints[0]
          .length));
      int randomIndex = new Random()
          .nextInt(this.svmJobParameters.getSamples() / dataStreamerParallelism - 1);
      LOG.info(String.format("Random DataPoint[%d] : %s", randomIndex, Arrays
          .toString(datapoints[randomIndex])));
    }

  }

  private void loadTestingData() {
    DataFlowTaskGraph testingDFTG = TGUtils.buildTestingDataPointsTG(this.dataStreamerParallelism,
        this.svmJobParameters, this.config, this.operationMode);
    ExecutionPlan testingEP = taskExecutor.plan(testingDFTG);
    taskExecutor.execute(testingDFTG, testingEP);
    testingDoubleDataPointObject = taskExecutor
        .getOutput(testingDFTG, testingEP,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING);
    for (int i = 0; i < testingDoubleDataPointObject.getPartitions().length; i++) {
      double[][] datapoints = testingDoubleDataPointObject.getPartitions()[i].getConsumer().next();
      LOG.info(String.format("Partition[%d] Testing Datapoints : %d,%d", i, datapoints.length,
          datapoints[0].length));
      int randomIndex = new Random()
          .nextInt(this.svmJobParameters.getSamples() / dataStreamerParallelism - 1);
      LOG.info(String.format("Random DataPoint[%d] : %s", randomIndex, Arrays
          .toString(datapoints[randomIndex])));
    }
  }

  private void loadWeightVector() {
    weightVectorTaskGraph = TGUtils.buildWeightVectorTG(config, dataStreamerParallelism,
        this.svmJobParameters, this.operationMode);
    weightVectorExecutionPlan = taskExecutor.plan(weightVectorTaskGraph);
    taskExecutor.execute(weightVectorTaskGraph, weightVectorExecutionPlan);
    inputDoubleWeightvectorObject = taskExecutor
        .getOutput(weightVectorTaskGraph, weightVectorExecutionPlan,
            Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);
    double[] w = inputDoubleWeightvectorObject.getPartitions()[0].getConsumer().next();
    LOG.info(String.format("Weight Vector Loaded : %s", Arrays.toString(w)));
  }

  private void generateSummary() {
    double totalMemory = ((double) Runtime.getRuntime().totalMemory()) / TimingConstants.B2MB;
    double maxMemory = ((double) Runtime.getRuntime().totalMemory()) / TimingConstants.B2MB;
    convert2Seconds();
    totalDTime = initializingDTime + dataLoadingDTime + trainingDTime + testingDTime;
    String s = "\n\n";
    s += "======================================================================================\n";
    s += "\t\t\tIterative SVM Task Summary : [" + this.experimentName + "]\n";
    s += "======================================================================================\n";
    s += "Training Dataset [" + this.svmJobParameters.getTrainingDataDir() + "] \n";
    s += "Testing  Dataset [" + this.svmJobParameters.getTestingDataDir() + "] \n";
    s += "Total Memory [ " + totalMemory + " MB] \n";
    s += "Maximum Memory [ " + maxMemory + " MB] \n";
    s += "Data Loading Time (Training + Testing) \t\t\t\t= " + String.format("%3.9f",
        dataLoadingDTime) + "  s \n";
    s += "Training Time \t\t\t\t\t\t\t= " + String.format("%3.9f", trainingDTime) + "  s \n";
    s += "Testing Time  \t\t\t\t\t\t\t= " + String.format("%3.9f", testingDTime) + "  s \n";
    s += "Total Time (Data Loading Time + Training Time + Testing Time) \t="
        + String.format(" %.9f", totalDTime) + "  s \n";
    s += String.format("Accuracy of the Trained Model \t\t\t\t\t= %2.9f", accuracy) + " %%\n";
    s += "======================================================================================\n";
    LOG.info(String.format(s));
  }

  private void convert2Seconds() {
    this.initializingDTime = this.initializingTime / TimingConstants.NANO_TO_SEC;
    this.dataLoadingDTime = this.dataLoadingTime / TimingConstants.NANO_TO_SEC;
    this.trainingDTime = this.trainingTime / TimingConstants.NANO_TO_SEC;
    this.testingDTime = this.testingTime / TimingConstants.NANO_TO_SEC;
  }

}
