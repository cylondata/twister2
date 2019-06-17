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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataFileReplicatedReadSource;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.ReduceAggregator;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.SVMReduce;
import edu.iu.dsc.tws.examples.ml.svm.compute.IterativeSVMCompute;
import edu.iu.dsc.tws.examples.ml.svm.compute.SVMCompute;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.streamer.InputDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeDataStream;
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
  private InputDataStreamer dataStreamer;
  private IterativeDataStream iterativeDataStream;
  private SVMCompute svmCompute;
  private IterativeSVMCompute iterativeSVMCompute;
  private SVMReduce svmReduce;
  private DataObject<Object> trainingData;
  private DataObject<Object> inputWeightVector;
  private DataObject<Object> testingData;
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
    this.binaryBatchModel.setIterations(this.svmJobParameters.getIterations());
    this.binaryBatchModel.setAlpha(this.svmJobParameters.getAlpha());
    this.binaryBatchModel.setFeatures(this.svmJobParameters.getFeatures());
    this.binaryBatchModel.setSamples(this.svmJobParameters.getSamples());
    this.binaryBatchModel.setW(DataUtils.seedDoubleArray(this.svmJobParameters.getFeatures()));
    LOG.info(this.binaryBatchModel.toString());
  }

  public void initializeExecute() {
    trainingBuilder = TaskGraphBuilder.newBuilder(config);
    testingBuilder = TaskGraphBuilder.newBuilder(config);

    this.operationMode = this.svmJobParameters.isStreaming()
        ? OperationMode.STREAMING : OperationMode.BATCH;

    inputWeightVector = executeWeightVectorLoadingTaskGraph();

    Long t1 = System.nanoTime();
    trainingData = executeTrainingDataLoadingTaskGraph();
    dataLoadingTime = (double) (System.nanoTime() - t1) / NANO_TO_SEC;

    t1 = System.nanoTime();
    executeIterativeTrainingGraph();
    trainingTime = (double) (System.nanoTime() - t1) / NANO_TO_SEC;

    t1 = System.nanoTime();
    testingData = executeTestingDataLoadingTaskGraph();
    dataLoadingTime += (double) (System.nanoTime() - t1) / NANO_TO_SEC;

  }


  public DataFlowTaskGraph builtSvmSgdIterativeTaskGraph(int parallelism, Config conf) {
    iterativeDataStream = new IterativeDataStream(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    svmReduce = new SVMReduce(this.operationMode);

    trainingBuilder.addSource(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
        iterativeDataStream, parallelism);
    ComputeConnection svmComputeConnection = trainingBuilder
        .addSink(Constants.SimpleGraphConfig.SVM_REDUCE, svmReduce, reduceParallelism);

    svmComputeConnection.reduce(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE)
        .viaEdge(Constants.SimpleGraphConfig.REDUCE_EDGE)
        .withReductionFunction(new ReduceAggregator())
        .withDataType(DataType.OBJECT);

    trainingBuilder.setMode(operationMode);
    trainingBuilder.setTaskGraphName("iterative-svm-sgd-taskgraph");

    return trainingBuilder.build();
  }

  /**
   * This method loads the training data in a distributed mode
   * dataStreamerParallelism is the amount of parallelism used
   * in loaded the data in parallel.
   *
   * @return twister2 DataObject containing the training data
   */
  public DataObject<Object> executeTrainingDataLoadingTaskGraph() {
    DataObject<Object> data = null;
    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getTrainingDataDir());
    DataObjectSink sinkTask = new DataObjectSink();
    trainingBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        sourceTask, dataStreamerParallelism);
    ComputeConnection firstGraphComputeConnection = trainingBuilder.addSink(
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK, sinkTask, dataStreamerParallelism);
    firstGraphComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE).withDataType(DataType.OBJECT);
    trainingBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph = trainingBuilder.build();
    datapointsTaskGraph.setGraphName("training-data-loading-graph");
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    data = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, Constants.SimpleGraphConfig.DATA_OBJECT_SINK);
    if (data == null) {
      throw new NullPointerException("Something Went Wrong in Loading Training Data");
    } else {
      LOG.info("Training Data Total Partitions : " + data.getPartitions().length);
    }
    return data;
  }

  /**
   * This method loads the training data in a distributed mode
   * dataStreamerParallelism is the amount of parallelism used
   * in loaded the data in parallel.
   *
   * @return twister2 DataObject containing the training data
   */
  public DataObject<Object> executeWeightVectorLoadingTaskGraph() {
    DataObject<Object> data = null;
    DataFileReplicatedReadSource sourceTask
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getWeightVectorDataDir());
    DataObjectSink sinkTask = new DataObjectSink();
    trainingBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        sourceTask, dataStreamerParallelism);
    ComputeConnection firstGraphComputeConnection = trainingBuilder.addSink(
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK, sinkTask, dataStreamerParallelism);
    firstGraphComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE)
        .viaEdge(Context.TWISTER2_DIRECT_EDGE).withDataType(DataType.OBJECT);
    trainingBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph = trainingBuilder.build();
    datapointsTaskGraph.setGraphName("weight-vector-loading-graph");
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    data = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, Constants.SimpleGraphConfig.DATA_OBJECT_SINK);
    if (data == null) {
      throw new NullPointerException("Something Went Wrong in Loading Weight Vector");
    } else {
      LOG.info("Training Data Total Partitions : " + data.getPartitions().length);
    }
    return data;
  }

  /**
   * This method loads the testing data
   * The loaded test data is used to evaluate the trained data
   * Testing data is loaded in parallel depending on the parallelism parameter given
   * There are partitions created equal to the parallelism
   * Later this will be used to do the testing in parallel in the testing task graph
   *
   * @return twister2 DataObject containing the testing data
   */
  public DataObject<Object> executeTestingDataLoadingTaskGraph() {
    DataObject<Object> data = null;
    final String TEST_DATA_LOAD_EDGE_DIRECT = "direct2";
    DataObjectSource sourceTask1 = new DataObjectSource(TEST_DATA_LOAD_EDGE_DIRECT,
        this.svmJobParameters.getTestingDataDir());
    DataObjectSink sinkTask1 = new DataObjectSink();
    testingBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
        sourceTask1, dataStreamerParallelism);
    ComputeConnection firstGraphComputeConnection1 = testingBuilder.addSink(
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING, sinkTask1, dataStreamerParallelism);
    firstGraphComputeConnection1.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING)
        .viaEdge(TEST_DATA_LOAD_EDGE_DIRECT).withDataType(DataType.OBJECT);
    testingBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph1 = testingBuilder.build();
    datapointsTaskGraph1.setGraphName("testing-data-loading-graph");
    ExecutionPlan firstGraphExecutionPlan1 = taskExecutor.plan(datapointsTaskGraph1);
    taskExecutor.execute(datapointsTaskGraph1, firstGraphExecutionPlan1);
    data = taskExecutor.getOutput(
        datapointsTaskGraph1, firstGraphExecutionPlan1,
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING);
    if (data == null) {
      throw new NullPointerException("Something Went Wrong in Loading Testing Data");
    } else {
      LOG.info("Testing Data Total Partitions : " + data.getPartitions().length);
    }
    return data;
  }

  public void executeIterativeTrainingGraph() {
    DataFlowTaskGraph svmTaskGraph
        = builtSvmSgdIterativeTaskGraph(this.dataStreamerParallelism, config);
    ExecutionPlan svmExecutionPlan = taskExecutor.plan(svmTaskGraph);

    for (int i = 0; i < this.binaryBatchModel.getIterations(); i++) {
      taskExecutor.addInput(
          svmTaskGraph, svmExecutionPlan, Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_DATA, trainingData);
      taskExecutor.addInput(svmTaskGraph, svmExecutionPlan,
          Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
          Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputWeightVector);

      taskExecutor.itrExecute(svmTaskGraph, svmExecutionPlan);

      inputWeightVector = taskExecutor.getOutput(svmTaskGraph, svmExecutionPlan,
          Constants.SimpleGraphConfig.SVM_REDUCE);
    }
    taskExecutor.waitFor(svmTaskGraph, svmExecutionPlan);
  }


  @Override
  public void execute() {
    initializeParameters();
    initializeExecute();
  }
}
