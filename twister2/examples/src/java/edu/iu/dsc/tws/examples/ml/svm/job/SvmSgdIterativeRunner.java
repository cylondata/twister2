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

import edu.iu.dsc.tws.api.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeAccuracyReduceFunction;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMAccuracyReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMWeightVectorReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeWeightVectorReduceFunction;
import edu.iu.dsc.tws.examples.ml.svm.config.DataPartitionType;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.constant.IterativeSVMConstants;
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
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SvmSgdIterativeRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdIterativeRunner.class.getName());



  private static final String DELIMITER = ",";
  private static final double NANO_TO_SEC = 1000000000;
  private static final double B2MB = 1024.0 * 1024.0;
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
  private double dataLoadingTime = 0;
  private double trainingTime = 0;
  private double testingTime = 0;
  private double accuracy = 0;
  private boolean debug = false;
  private String experimentName = "";


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

  /**
   * *******************************************************************************************
   * Start: initializing test execution
   * *******************************************************************************************
   */

  public SvmSgdIterativeRunner initialize() {
    initializeParameters();
    return this;
  }

  public SvmSgdIterativeRunner loadData() {
    loadTrainingData();
    loadTestingData();
    return this;
  }

  public SvmSgdIterativeRunner withWeightVector() {
    loadWeightVector();
    return this;
  }

  public SvmSgdIterativeRunner train() {
    withWeightVector();
    runTrainingGraph();
    return this;
  }

  public SvmSgdIterativeRunner predict() {
    runPredictionGraph();
    return this;
  }

  // TODO: Bundle this with the data partition logic in data API
  public void testDataPartitionLogic() {
    HashMap<Integer, Integer> mapOfTaskIndexAndDatapoints = new DataPartitioner()
        .withParallelism(this.dataStreamerParallelism)
        .withSamples(this.svmJobParameters.getSamples())
        .withPartitionType(DataPartitionType.DEFAULT)
        .withImbalancePartitionId(1)
        .partition()
        .getDataPartitionMap();

    LOG.info(String.format("Map Info : %s", mapOfTaskIndexAndDatapoints.toString()));
  }

  public void loadWeightVector() {
    weightVectorTaskGraph = buildWeightVectorTG();
    weightVectorExecutionPlan = taskExecutor.plan(weightVectorTaskGraph);
    taskExecutor.execute(weightVectorTaskGraph, weightVectorExecutionPlan);
    inputDoubleWeightvectorObject = taskExecutor
        .getOutput(weightVectorTaskGraph, weightVectorExecutionPlan,
            Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);
    double[] w = inputDoubleWeightvectorObject.getPartitions()[0].getConsumer().next();
    LOG.info(String.format("Weight Vector Loaded : %s", Arrays.toString(w)));
  }

  private void loadTrainingData() {
    DataFlowTaskGraph trainingDatapointsTaskGraph = buildTrainingDataPointsTG();
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

  public void loadTestingData() {
    DataFlowTaskGraph testingDatapointsTaskGraph = buildTestingDataPointsTG();
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
          .nextInt(this.svmJobParameters.getSamples() / dataStreamerParallelism - 1);
      LOG.info(String.format("Random DataPoint[%d] : %s", randomIndex, Arrays
          .toString(datapoints[randomIndex])));
    }

  }

  public DataFlowTaskGraph buildTrainingDataPointsTG() {
    return generateGenericDataPointLoader(this.svmJobParameters.getSamples(),
        this.dataStreamerParallelism, this.svmJobParameters.getFeatures(),
        this.svmJobParameters.getTrainingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK,
        IterativeSVMConstants.TRAINING_DATA_LOADING_TASK_GRAPH);

  }

  public DataFlowTaskGraph buildTestingDataPointsTG() {
    return generateGenericDataPointLoader(this.svmJobParameters.getTestingSamples(),
        this.dataStreamerParallelism, this.svmJobParameters.getFeatures(),
        this.svmJobParameters.getTestingDataDir(),
        Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE_TESTING,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING,
        IterativeSVMConstants.TESTING_DATA_LOADING_TASK_GRAPH);

  }

  public DataFlowTaskGraph generateGenericDataPointLoader(int samples, int parallelism,
                                                          int numOfFeatures,
                                                          String dataSourcePathStr,
                                                          String dataObjectSourceStr,
                                                          String dataObjectComputeStr,
                                                          String dataObjectSinkStr,
                                                          String graphName) {
    SVMDataObjectSource<String, TextInputSplit> sourceTask
        = new SVMDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataSourcePathStr);
    IterativeSVMDataObjectCompute dataObjectCompute
        = new IterativeSVMDataObjectCompute(Context.TWISTER2_DIRECT_EDGE, parallelism,
        samples, numOfFeatures, DELIMITER);
    IterativeSVMDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    datapointsTaskGraphBuilder.addSource(dataObjectSourceStr,
        sourceTask,
        parallelism);
    ComputeConnection datapointComputeConnection
        = datapointsTaskGraphBuilder.addCompute(dataObjectComputeStr,
        dataObjectCompute, parallelism);
    ComputeConnection computeConnectionSink = datapointsTaskGraphBuilder
        .addSink(dataObjectSinkStr,
            iterativeSVMPrimaryDataObjectDirectSink,
            parallelism);
    datapointComputeConnection.direct(dataObjectSourceStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(dataObjectComputeStr)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(this.operationMode);

    datapointsTaskGraphBuilder.setTaskGraphName(graphName);
    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();
  }


  public DataFlowTaskGraph buildWeightVectorTG() {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getWeightVectorDataDir());
    IterativeSVMWeightVectorObjectCompute weightVectorObjectCompute
        = new IterativeSVMWeightVectorObjectCompute(Context.TWISTER2_DIRECT_EDGE, 1,
        this.svmJobParameters.getFeatures());
    IterativeSVMWeightVectorObjectDirectSink weightVectorObjectSink
        = new IterativeSVMWeightVectorObjectDirectSink();
    TaskGraphBuilder weightVectorTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    weightVectorTaskGraphBuilder
        .addSource(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE,
            dataFileReplicatedReadSource, dataStreamerParallelism);
    ComputeConnection weightVectorComputeConnection = weightVectorTaskGraphBuilder
        .addCompute(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE,
            weightVectorObjectCompute, dataStreamerParallelism);
    ComputeConnection weightVectorSinkConnection = weightVectorTaskGraphBuilder
        .addSink(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK, weightVectorObjectSink,
            dataStreamerParallelism);

    weightVectorComputeConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    weightVectorSinkConnection.direct(Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_COMPUTE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.DOUBLE_ARRAY);
    weightVectorTaskGraphBuilder.setMode(operationMode);
    weightVectorTaskGraphBuilder
        .setTaskGraphName(IterativeSVMConstants.WEIGHT_VECTOR_LOADING_TASK_GRAPH);

    return weightVectorTaskGraphBuilder.build();
  }

  public void runTrainingGraph() {
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

  public DataFlowTaskGraph buildSvmSgdIterativeTrainingTG() {
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

  public void runPredictionGraph() {
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

  public DataFlowTaskGraph buildSvmSgdTestingTG() {
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

  /**
   * *******************************************************************************************
   * END : initializing test execution
   * *******************************************************************************************
   */



  @Override
  public void execute() {
    //method 1
//    initializeParameters();
//    testDataPartitionLogic();
//    loadWeightVector();
//    loadTrainingData();
//    loadTestingData();
//    runTrainingGraph();
//    runPredictionGraph();
    this.initialize()
        .loadData()
        .train()
        .predict();
  }
}
