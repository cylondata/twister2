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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.data.api.splits.TextInputSplit;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeTrainingReduce;
import edu.iu.dsc.tws.examples.ml.svm.compute.IterativeSVMCompute;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMDataObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMPrimaryDataObjectDirectSink;
import edu.iu.dsc.tws.examples.ml.svm.data.IterativeSVMPrimaryWeightVectorObjectCompute;
import edu.iu.dsc.tws.examples.ml.svm.data.SVMDataObjectSource;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IterativeSVMDataObjectCompute1;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IterativeSVMWeightVectorObjectCompute1;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IterativeSVMWeightVectorObjectDirectSink1;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeDataStream;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativePredictionDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionAggregator;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionReduceTask;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionSourceTask;
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
  private final int reduceParallelism = 1;
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
  private IterativeSVMCompute iterativeSVMCompute;
  private IterativeSVMReduce iterativeSVMReduce;
  private DataObject<Object> trainingDataPointObject;
  private DataObject<double[][]> trainingDoubleDataPointObject;
  private DataObject<Object> inputweightvectorObject;
  private DataObject<double[]> inputDoubleWeightvectorObject;
  private DataObject<double[][]> finalAccuracyObject;
  private DataObject<double[][]> finalweightvectorObject;
  private DataObject<Object> testingDataPointObject;
  private DataObject<Object> testingResults;
  private DataObject<double[]> trainedWeightVector;
  private PredictionSourceTask predictionSourceTask;
  private PredictionReduceTask predictionReduceTask;
  private PredictionAggregator predictionAggregator;
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
   * initializing test execution
   */

  public void testGenericWeightVectorLoad() {
    weightVectorTaskGraph = buildTestWeightVectorTG();
    weightVectorExecutionPlan = taskExecutor.plan(weightVectorTaskGraph);
    taskExecutor.execute(weightVectorTaskGraph, weightVectorExecutionPlan);
    inputDoubleWeightvectorObject = taskExecutor
        .getOutput(weightVectorTaskGraph, weightVectorExecutionPlan,
            Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);
    double[] w = inputDoubleWeightvectorObject.getPartitions()[0].getConsumer().next();
    LOG.info(String.format("Weight Vector Loaded : %s", Arrays.toString(w)));
  }

  public void testGenericTrainingDataLoad() {
    DataFlowTaskGraph trainingDatapointsTaskGraph = buildTestGenricTrainingDataPointsTG();
    ExecutionPlan datapointsExecutionPlan = taskExecutor.plan(trainingDatapointsTaskGraph);
    taskExecutor.execute(trainingDatapointsTaskGraph, datapointsExecutionPlan);
    trainingDataPointObject = taskExecutor
        .getOutput(trainingDatapointsTaskGraph, datapointsExecutionPlan,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK);

    Object o = trainingDataPointObject.getPartitions()[0].getConsumer().next();
    LOG.info(String.format("O : %s", o.getClass().getName()));
  }

  /**
   * Initializing Execute
   */
  public void initializeExecute() {
    DataFlowTaskGraph trainingDatapointsTaskGraph = buildTrainingDataPointsTG();
    ExecutionPlan datapointsExecutionPlan = taskExecutor.plan(trainingDatapointsTaskGraph);
    taskExecutor.execute(trainingDatapointsTaskGraph, datapointsExecutionPlan);
    trainingDataPointObject = taskExecutor
        .getOutput(trainingDatapointsTaskGraph, datapointsExecutionPlan,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK);

    weightVectorTaskGraph = buildWeightVectorTG();
    weightVectorExecutionPlan = taskExecutor.plan(weightVectorTaskGraph);
    taskExecutor.execute(weightVectorTaskGraph, weightVectorExecutionPlan);
    inputweightvectorObject = taskExecutor
        .getOutput(weightVectorTaskGraph, weightVectorExecutionPlan,
            Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);

    DataFlowTaskGraph testingDatapointsTaskGraph = buildTestingDataPointsTG();
    ExecutionPlan testingDatapointsExecutionPlan = taskExecutor.plan(testingDatapointsTaskGraph);
    taskExecutor.execute(testingDatapointsTaskGraph, testingDatapointsExecutionPlan);
    testingDataPointObject = taskExecutor
        .getOutput(testingDatapointsTaskGraph, testingDatapointsExecutionPlan,
            Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING);

    int dtrlength = trainingDataPointObject.getPartitionCount();
    int plength = inputweightvectorObject.getPartitionCount();
    int dtslength = testingDataPointObject.getPartitionCount();
//    LOG.info(String.format("Weight Partitions %d, Data Partitions Tr : %d, Ts : %d",
//        plength, dtrlength, dtslength));

    executeIterativeTrainingGraph();

    double[] wFinal = retrieveWeightVector(iterativeSVMTrainingTaskGraph,
        iterativeSVMTrainingExecutionPlan,
        Constants.SimpleGraphConfig.ITERATIVE_SVM_REDUCE);
    double[] wInitial = retrieveWeightVector(weightVectorTaskGraph, weightVectorExecutionPlan,
        Constants.SimpleGraphConfig.WEIGHT_VECTOR_OBJECT_SINK);
//    LOG.info(String.format("Initial Weight Vector : %s", Arrays.toString(wInitial)));
//    LOG.info(String.format("Final Weight Vector : %s", Arrays.toString(wFinal)));

    executeIterativeTestingGraph();

  }


  public DataFlowTaskGraph builtSvmSgdIterativeTrainingTG() {
    iterativeDataStream = new IterativeDataStream(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    iterativeSVMReduce = new IterativeSVMReduce(this.operationMode);

    trainingBuilder.addSource(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
        iterativeDataStream, dataStreamerParallelism);
    ComputeConnection svmComputeConnection = trainingBuilder
        .addSink(Constants.SimpleGraphConfig.ITERATIVE_SVM_REDUCE, iterativeSVMReduce,
            dataStreamerParallelism);

    svmComputeConnection.allreduce(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE)
        .viaEdge(Constants.SimpleGraphConfig.REDUCE_EDGE)
        .withReductionFunction(new IterativeTrainingReduce())
        .withDataType(MessageTypes.OBJECT);

    trainingBuilder.setMode(operationMode);
    trainingBuilder.setTaskGraphName("iterative-svm-sgd-taskgraph");

    return trainingBuilder.build();
  }

  public DataFlowTaskGraph buildSvmSgdTestingTG() {
    iterativePredictionDataStreamer
        = new IterativePredictionDataStreamer(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    iterativeSVMReduce = new IterativeSVMReduce(this.operationMode);

    testingBuilder.addSource(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        iterativePredictionDataStreamer, dataStreamerParallelism);
    ComputeConnection svmComputeConnection = testingBuilder
        .addSink(Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK, iterativeSVMReduce,
            dataStreamerParallelism);

    svmComputeConnection.allreduce(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK)
        .viaEdge(Constants.SimpleGraphConfig.PREDICTION_EDGE)
        .withReductionFunction(new IterativeTrainingReduce())
        .withDataType(MessageTypes.OBJECT);

    testingBuilder.setMode(operationMode);
    testingBuilder.setTaskGraphName("iterative-svm-sgd-prediction-taskgraph");

    return testingBuilder.build();
  }

  /**
   * Iterative Method Support   *
   */

  public DataFlowTaskGraph buildTrainingDataPointsTG() {
    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getTrainingDataDir());
    IterativeSVMDataObjectCompute dataObjectCompute
        = new IterativeSVMDataObjectCompute(Context.TWISTER2_DIRECT_EDGE, dataStreamerParallelism,
        this.svmJobParameters.getSamples(), this.svmJobParameters.getFeatures());
    IterativeSVMPrimaryDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMPrimaryDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    datapointsTaskGraphBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE, sourceTask,
        dataStreamerParallelism);
    ComputeConnection datapointComputeConnection
        = datapointsTaskGraphBuilder.addCompute(Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE,
        dataObjectCompute, dataStreamerParallelism);
    ComputeConnection computeConnectionSink = datapointsTaskGraphBuilder
        .addSink(Constants.SimpleGraphConfig.DATA_OBJECT_SINK,
            iterativeSVMPrimaryDataObjectDirectSink,
            dataStreamerParallelism);
    datapointComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(this.operationMode);

    datapointsTaskGraphBuilder.setTaskGraphName("trainingDatapointsTG");
    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();

  }

  public DataFlowTaskGraph buildTestGenricTrainingDataPointsTG() {
    SVMDataObjectSource<String, TextInputSplit> sourceTask
        = new SVMDataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getTrainingDataDir());
    IterativeSVMDataObjectCompute1 dataObjectCompute
        = new IterativeSVMDataObjectCompute1(Context.TWISTER2_DIRECT_EDGE, dataStreamerParallelism,
        this.svmJobParameters.getSamples(), this.svmJobParameters.getFeatures(), DELIMITER);
    IterativeSVMPrimaryDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMPrimaryDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    datapointsTaskGraphBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE, sourceTask,
        dataStreamerParallelism);
    ComputeConnection datapointComputeConnection
        = datapointsTaskGraphBuilder.addCompute(Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE,
        dataObjectCompute, dataStreamerParallelism);
    ComputeConnection computeConnectionSink = datapointsTaskGraphBuilder
        .addSink(Constants.SimpleGraphConfig.DATA_OBJECT_SINK,
            iterativeSVMPrimaryDataObjectDirectSink,
            dataStreamerParallelism);
    datapointComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(this.operationMode);

    datapointsTaskGraphBuilder.setTaskGraphName("trainingDatapointsTG");
    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();

  }


  public DataFlowTaskGraph buildTestingDataPointsTG() {
    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getTestingDataDir());
    IterativeSVMDataObjectCompute dataObjectCompute
        = new IterativeSVMDataObjectCompute(Context.TWISTER2_DIRECT_EDGE, dataStreamerParallelism,
        this.svmJobParameters.getSamples(), this.svmJobParameters.getFeatures());
    IterativeSVMPrimaryDataObjectDirectSink iterativeSVMPrimaryDataObjectDirectSink
        = new IterativeSVMPrimaryDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    datapointsTaskGraphBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
        sourceTask,
        dataStreamerParallelism);
    ComputeConnection datapointComputeConnection
        = datapointsTaskGraphBuilder.addCompute(Constants.SimpleGraphConfig
            .DATA_OBJECT_COMPUTE_TESTING,
        dataObjectCompute, dataStreamerParallelism);
    ComputeConnection computeConnectionSink = datapointsTaskGraphBuilder
        .addSink(Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING,
            iterativeSVMPrimaryDataObjectDirectSink,
            dataStreamerParallelism);
    datapointComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    computeConnectionSink.direct(Constants.SimpleGraphConfig.DATA_OBJECT_COMPUTE_TESTING)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(this.operationMode);

    datapointsTaskGraphBuilder.setTaskGraphName("testingDatapointsTG");
    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();

  }

  public DataFlowTaskGraph buildWeightVectorTG() {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getWeightVectorDataDir());
    IterativeSVMPrimaryWeightVectorObjectCompute weightVectorObjectCompute
        = new IterativeSVMPrimaryWeightVectorObjectCompute(Context.TWISTER2_DIRECT_EDGE, 1,
        this.svmJobParameters.getFeatures());
    IterativeSVMPrimaryDataObjectDirectSink weightVectorObjectSink
        = new IterativeSVMPrimaryDataObjectDirectSink();
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
        .withDataType(MessageTypes.OBJECT);
    weightVectorTaskGraphBuilder.setMode(operationMode);
    weightVectorTaskGraphBuilder.setTaskGraphName("weight-vector-task-graph");

    return weightVectorTaskGraphBuilder.build();
  }

  public DataFlowTaskGraph buildTestWeightVectorTG() {
    DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getWeightVectorDataDir());
    IterativeSVMWeightVectorObjectCompute1 weightVectorObjectCompute
        = new IterativeSVMWeightVectorObjectCompute1(Context.TWISTER2_DIRECT_EDGE, 1,
        this.svmJobParameters.getFeatures());
    IterativeSVMWeightVectorObjectDirectSink1 weightVectorObjectSink
        = new IterativeSVMWeightVectorObjectDirectSink1();
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
    weightVectorTaskGraphBuilder.setTaskGraphName("weight-vector-test-task-graph");

    return weightVectorTaskGraphBuilder.build();
  }


  public void executeIterativeTrainingGraph() {
    iterativeSVMTrainingTaskGraph = builtSvmSgdIterativeTrainingTG();
    iterativeSVMTrainingExecutionPlan = taskExecutor.plan(iterativeSVMTrainingTaskGraph);

    for (int i = 0; i < this.binaryBatchModel.getIterations(); i++) {
      LOG.info(String.format("Iteration  %d ", i));
      taskExecutor.addInput(
          iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_DATA, trainingDataPointObject);
      taskExecutor.addInput(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputweightvectorObject);

      taskExecutor.itrExecute(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan);

      inputweightvectorObject = taskExecutor.getOutput(iterativeSVMTrainingTaskGraph,
          iterativeSVMTrainingExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_SVM_REDUCE);
    }
    taskExecutor.waitFor(iterativeSVMTrainingTaskGraph, iterativeSVMTrainingExecutionPlan);
  }

  public void executeIterativeTestingGraph() {
    iterativeSVMTestingTaskGraph = buildSvmSgdTestingTG();
    iterativeSVMTestingExecutionPlan = taskExecutor.plan(iterativeSVMTestingTaskGraph);

    taskExecutor.addInput(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        Constants.SimpleGraphConfig.TEST_DATA, testingDataPointObject);
    taskExecutor.addInput(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
        Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputweightvectorObject);
    taskExecutor.execute(iterativeSVMTestingTaskGraph, iterativeSVMTestingExecutionPlan);
    finalAccuracyObject = taskExecutor.getOutput(iterativeSVMTestingTaskGraph,
        iterativeSVMTestingExecutionPlan,
        Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK);
    accuracy = finalAccuracyObject.getPartitions()[0].getConsumer().next()[0][0];
    LOG.info(String.format("Final Accuracy : %f ", accuracy));

  }


  public double[] retrieveWeightVector(DataFlowTaskGraph graph, ExecutionPlan plan,
                                       String taskName) {
    DataObject<double[][]> dataSet = taskExecutor.getOutput(graph,
        plan,
        taskName);
    if (debug) {
      LOG.info(String.format("Number of Partitions : %d ", dataSet.getPartitions().length));
    }

    DataPartition<double[][]> values = dataSet.getPartitions()[0];
    DataPartitionConsumer<double[][]> dataPartitionConsumer = values.getConsumer();
    return dataPartitionConsumer.next()[0];
  }


  @Override
  public void execute() {
    initializeParameters();
    //initializeExecute();
    //testGenericWeightVectorLoad();
    testGenericTrainingDataLoad();
  }
}
