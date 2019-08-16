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
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeAccuracyReduceFunction;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMAccuracyReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMWeightVectorReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeWeightVectorReduceFunction;
import edu.iu.dsc.tws.examples.ml.svm.config.DataPartitionType;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.constant.IterativeSVMConstants;
import edu.iu.dsc.tws.examples.ml.svm.constant.TimingConstants;
import edu.iu.dsc.tws.examples.ml.svm.data.DataPartitioner;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMDataObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMDataObjectDirectSink;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMWeightVectorObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMWeightVectorObjectDirectSink;
import edu.iu.dsc.tws.examples.ml.svm.data.SVMDataObjectSource;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeDataStream;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativePredictionDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TrainedModel;
import edu.iu.dsc.tws.task.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

public class SvmSgdIterativeRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdIterativeRunner.class.getName());

  private static final String DELIMITER = ",";
  private int dataStreamerParallelism = 4;
  private int svmComputeParallelism = 4;
  private int features = 10;
  private OperationMode operationMode;
  private SVMJobParameters svmJobParameters;
  private BinaryBatchModel binaryBatchModel;
  private ComputeGraphBuilder trainingBuilder;
  private ComputeGraphBuilder testingBuilder;
  private ComputeGraph iterativeSVMTrainingTaskGraph;
  private ExecutionPlan iterativeSVMTrainingExecutionPlan;
  private ComputeGraph iterativeSVMTestingTaskGraph;
  private ExecutionPlan iterativeSVMTestingExecutionPlan;
  private ComputeGraph weightVectorTaskGraph;
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
    // method 1
//    initializeParameters();
//    testDataPartitionLogic();
//    loadWeightVector();
//    loadTrainingData();
//    loadTestingData();
//    runTrainingGraph();
//    runPredictionGraph();
//    generateSummary();

    // method 2
    this.initialize()
        .loadData()
        .train()
        .predict()
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
    trainingBuilder = ComputeGraphBuilder.newBuilder(config);
    testingBuilder = ComputeGraphBuilder.newBuilder(config);
  }

  /**
   * *******************************************************************************************
   * Start
   * *******************************************************************************************
   */

  public SvmSgdIterativeRunner initialize() {
    long t1 = System.nanoTime();
    initializeParameters();
    this.initializingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdIterativeRunner loadData() {
    long t1 = System.nanoTime();
    loadTrainingData();
    loadTestingData();
    this.dataLoadingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdIterativeRunner withWeightVector() {
    long t1 = System.nanoTime();
    loadWeightVector();
    this.dataLoadingTime += System.nanoTime() - t1;
    return this;
  }

  public SvmSgdIterativeRunner train() {
    withWeightVector();
    long t1 = System.nanoTime();
    runTrainingGraph();
    this.trainingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdIterativeRunner predict() {
    long t1 = System.nanoTime();
    runPredictionGraph();
    this.testingTime = System.nanoTime() - t1;
    return this;
  }

  public SvmSgdIterativeRunner summary() {
    generateSummary();
    return this;
  }

  // TODO: Bundle this with the data partition logic in data API
  private void testDataPartitionLogic() {
    HashMap<Integer, Integer> mapOfTaskIndexAndDatapoints = new DataPartitioner()
        .withParallelism(this.dataStreamerParallelism)
        .withSamples(this.svmJobParameters.getSamples())
        .withPartitionType(DataPartitionType.DEFAULT)
        .withImbalancePartitionId(1)
        .partition()
        .getDataPartitionMap();

    LOG.info(String.format("Map Info : %s", mapOfTaskIndexAndDatapoints.toString()));
  }

  private void loadWeightVector() {
    weightVectorTaskGraph = buildWeightVectorTG();
    weightVectorExecutionPlan = taskExecutor.plan(weightVectorTaskGraph);
    taskExecutor.execute(weightVectorTaskGraph, weightVectorExecutionPlan);
    inputDoubleWeightvectorObject = taskExecutor
        .getOutput(weightVectorTaskGraph, weightVectorExecutionPlan,
            Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);
    double[] w = inputDoubleWeightvectorObject.getPartitions()[0].getConsumer().next();
    // TODO : initial load of weight vector is faulty test it
    // Doesn't affect training
    LOG.info(String.format("Weight Vector Loaded : %s", Arrays.toString(w)));
  }

  private void loadTrainingData() {
    ComputeGraph trainingDatapointsTaskGraph = buildTrainingDataPointsTG();
    ExecutionPlan datapointsExecutionPlan = taskExecutor.plan(trainingDatapointsTaskGraph);
    taskExecutor.execute(trainingDatapointsTaskGraph, datapointsExecutionPlan);
    trainingDoubleDataPointObject = taskExecutor
        .getOutput(trainingDatapointsTaskGraph, datapointsExecutionPlan,
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
    ComputeGraph testingDatapointsTaskGraph = buildTestingDataPointsTG();
    ExecutionPlan datapointsExecutionPlan = taskExecutor.plan(testingDatapointsTaskGraph);
    taskExecutor.execute(testingDatapointsTaskGraph, datapointsExecutionPlan);
    testingDoubleDataPointObject = taskExecutor
        .getOutput(testingDatapointsTaskGraph, datapointsExecutionPlan,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING);

    for (int i = 0; i < testingDoubleDataPointObject.getPartitions().length; i++) {
      double[][] datapoints = testingDoubleDataPointObject.getPartitions()[i].getConsumer().next();
      LOG.info(String.format("Partition[%d] Testing Datapoints : %d,%d", i, datapoints.length,
          datapoints[0].length));
      int randomIndex = new Random()
          .nextInt(this.svmJobParameters.getTestingSamples() / dataStreamerParallelism - 1);
      LOG.info(String.format("Random DataPoint[%d] : %s", randomIndex, Arrays
          .toString(datapoints[randomIndex])));
    }

  }

  private ComputeGraph buildTrainingDataPointsTG() {
    return generateGenericDataPointLoader(this.svmJobParameters.getSamples(),
        this.dataStreamerParallelism, this.svmJobParameters.getFeatures(),
        this.svmJobParameters.getTrainingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK,
        IterativeSVMConstants.TRAINING_DATA_LOADING_TASK_GRAPH);

  }

  private ComputeGraph buildTestingDataPointsTG() {
    return generateGenericDataPointLoader(this.svmJobParameters.getTestingSamples(),
        this.dataStreamerParallelism, this.svmJobParameters.getFeatures(),
        this.svmJobParameters.getTestingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING,
        IterativeSVMConstants.TESTING_DATA_LOADING_TASK_GRAPH);

  }

  private ComputeGraph generateGenericDataPointLoader(int samples, int parallelism,
                                                      int numOfFeatures,
                                                      String dataSourcePathStr,
                                                      String dataObjectSourceStr,
                                                      String dataObjectComputeStr,
                                                      String dataObjectSinkStr,
                                                      String graphName) {
    SVMDataObjectSource<String, TextInputSplit> sourceTask
        = new SVMDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataSourcePathStr, samples);
    IterativeSVMDataObjectCompute dataObjectCompute
        = new IterativeSVMDataObjectCompute(Context.TWISTER2_DIRECT_EDGE, parallelism,
        samples, numOfFeatures, DELIMITER);
    IterativeSVMDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMDataObjectDirectSink();
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    datapointsComputeGraphBuilder.addSource(dataObjectSourceStr,
        sourceTask,
        parallelism);
    ComputeConnection datapointComputeConnection
        = datapointsComputeGraphBuilder.addCompute(dataObjectComputeStr,
        dataObjectCompute, parallelism);
    ComputeConnection computeConnectionSink = datapointsComputeGraphBuilder
        .addSink(dataObjectSinkStr,
            iterativeSVMPrimaryDataObjectDirectSink,
            parallelism);
    datapointComputeConnection.direct(dataObjectSourceStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(dataObjectComputeStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(this.operationMode);

    datapointsComputeGraphBuilder.setTaskGraphName(graphName);
    //Build the first taskgraph
    return datapointsComputeGraphBuilder.build();
  }


  private ComputeGraph buildWeightVectorTG() {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getWeightVectorDataDir(), 1);
    IterativeSVMWeightVectorObjectCompute weightVectorObjectCompute
        = new IterativeSVMWeightVectorObjectCompute(Context.TWISTER2_DIRECT_EDGE, 1,
        this.svmJobParameters.getFeatures());
    IterativeSVMWeightVectorObjectDirectSink weightVectorObjectSink
        = new IterativeSVMWeightVectorObjectDirectSink();
    ComputeGraphBuilder weightVectorComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    weightVectorComputeGraphBuilder
        .addSource(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE,
            dataFileReplicatedReadSource, dataStreamerParallelism);
    ComputeConnection weightVectorComputeConnection = weightVectorComputeGraphBuilder
        .addCompute(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE,
            weightVectorObjectCompute, dataStreamerParallelism);
    ComputeConnection weightVectorSinkConnection = weightVectorComputeGraphBuilder
        .addSink(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK, weightVectorObjectSink,
            dataStreamerParallelism);

    weightVectorComputeConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    weightVectorSinkConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.DOUBLE_ARRAY);
    weightVectorComputeGraphBuilder.setMode(operationMode);
    weightVectorComputeGraphBuilder
        .setTaskGraphName(IterativeSVMConstants.WEIGHT_VECTOR_LOADING_TASK_GRAPH);

    return weightVectorComputeGraphBuilder.build();
  }

  private void runTrainingGraph() {
    iterativeSVMTrainingTaskGraph = buildSvmSgdIterativeTrainingTG();
    iterativeSVMTrainingExecutionPlan = taskExecutor.plan(iterativeSVMTrainingTaskGraph);

    for (int i = 0; i < this.binaryBatchModel.getIterations(); i++) {
      LOG.info(String.format("Iteration  %d ", i));
      taskExecutor.addInput(
          iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_DATA, trainingDoubleDataPointObject);
      taskExecutor.addInput(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputDoubleWeightvectorObject);

      taskExecutor.itrExecute(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan);

      inputDoubleWeightvectorObject = taskExecutor.getOutput(iterativeSVMTrainingTaskGraph,
          iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_SVM_REDUCE);
    }
    taskExecutor.waitFor(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan);
  }

  private ComputeGraph buildSvmSgdIterativeTrainingTG() {
    iterativeDataStream = new IterativeDataStream(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    iterativeSVMRiterativeSVMWeightVectorReduce
        = new IterativeSVMWeightVectorReduce(this.operationMode);

    trainingBuilder.addSource(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
        iterativeDataStream, dataStreamerParallelism);
    ComputeConnection svmComputeConnection = trainingBuilder
        .addSink(Constants.SimpleGraphConfig.ITERATIVE_SVM_REDUCE,
            iterativeSVMRiterativeSVMWeightVectorReduce,
            dataStreamerParallelism);

    svmComputeConnection.allreduce(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE)
        .viaEdge(Constants.SimpleGraphConfig.REDUCE_EDGE)
        .withReductionFunction(new IterativeWeightVectorReduceFunction())
        .withDataType(MessageTypes.DOUBLE_ARRAY);

    trainingBuilder.setMode(operationMode);
    trainingBuilder.setTaskGraphName(IterativeSVMConstants.ITERATIVE_TRAINING_TASK_GRAPH);

    return trainingBuilder.build();
  }

  private void runPredictionGraph() {
    iterativeSVMTestingTaskGraph = buildSvmSgdTestingTG();
    iterativeSVMTestingExecutionPlan = taskExecutor.plan(iterativeSVMTestingTaskGraph);

    taskExecutor.addInput(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        Constants.SimpleGraphConfig.TEST_DATA, testingDoubleDataPointObject);
    taskExecutor.addInput(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputDoubleWeightvectorObject);
    taskExecutor.execute(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan);
    finalAccuracyDoubleObject = taskExecutor.getOutput(iterativeSVMTestingTaskGraph,
        iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK);
    accuracy = finalAccuracyDoubleObject.getPartitions()[0].getConsumer().next();
    LOG.info(String.format("Final Accuracy : %f ", accuracy));
  }

  private ComputeGraph buildSvmSgdTestingTG() {
    iterativePredictionDataStreamer
        = new IterativePredictionDataStreamer(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    iterativeSVMAccuracyReduce = new IterativeSVMAccuracyReduce(this.operationMode);

    testingBuilder.addSource(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        iterativePredictionDataStreamer, dataStreamerParallelism);
    ComputeConnection svmComputeConnection = testingBuilder
        .addSink(Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK, iterativeSVMAccuracyReduce,
            dataStreamerParallelism);

    svmComputeConnection.allreduce(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK)
        .viaEdge(Constants.SimpleGraphConfig.PREDICTION_EDGE)
        .withReductionFunction(new IterativeAccuracyReduceFunction())
        .withDataType(MessageTypes.DOUBLE);

    testingBuilder.setMode(operationMode);
    testingBuilder.setTaskGraphName(IterativeSVMConstants.ITERATIVE_PREDICTION_TASK_GRAPH);

    return testingBuilder.build();
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
    save();
  }

  private void save() {
    TrainedModel trainedModel = new TrainedModel(this.binaryBatchModel, accuracy, this.trainingTime,
        this.svmJobParameters.getExperimentName() + "-itr-task", this.svmJobParameters
        .getParallelism());
    trainedModel.saveModel(this.svmJobParameters.getModelSaveDir());
  }

  private void convert2Seconds() {
    this.initializingDTime = this.initializingTime / TimingConstants.NANO_TO_SEC;
    this.dataLoadingDTime = this.dataLoadingTime / TimingConstants.NANO_TO_SEC;
    this.trainingDTime = this.trainingTime / TimingConstants.NANO_TO_SEC;
    this.testingDTime = this.testingTime / TimingConstants.NANO_TO_SEC;
  }

  /**
   * *******************************************************************************************
   * END
   * *******************************************************************************************
   */
}
