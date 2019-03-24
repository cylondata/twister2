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

import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.ReduceAggregator;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.SVMReduce;
import edu.iu.dsc.tws.examples.ml.svm.compute.SVMCompute;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.streamer.InputDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionAggregator;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionReduceTask;
import edu.iu.dsc.tws.examples.ml.svm.test.PredictionSourceTask;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SvmSgdAdvancedRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdAdvancedRunner.class.getName());

  private int dataStreamerParallelism = 4;

  private int svmComputeParallelism = 4;

  private final int reduceParallelism = 1;

  private int features = 10;

  private OperationMode operationMode;

  private SVMJobParameters svmJobParameters;

  private BinaryBatchModel binaryBatchModel;


  @Override
  public void execute() {
    initializeParameters();
    initializeExecute();
  }

  public void initializeParameters() {
    this.svmJobParameters = SVMJobParameters.build(config);
    this.binaryBatchModel = new BinaryBatchModel();
    this.binaryBatchModel.setIterations(this.svmJobParameters.getIterations());
    this.binaryBatchModel.setAlpha(this.svmJobParameters.getAlpha());
    this.binaryBatchModel.setFeatures(this.svmJobParameters.getFeatures());
    this.binaryBatchModel.setSamples(this.svmJobParameters.getSamples());
    this.binaryBatchModel.setW(DataUtils.seedDoubleArray(this.svmJobParameters.getFeatures()));
    LOG.info(this.binaryBatchModel.toString());

  }

  /**
   * Initializing the execute method
   */
  public void initializeExecute() {
    TaskGraphBuilder trainingBuilder = TaskGraphBuilder.newBuilder(config);

    this.operationMode = this.svmJobParameters.isStreaming()
        ? OperationMode.STREAMING : OperationMode.BATCH;

    //TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        this.svmJobParameters.getTrainingDataDir());
    DataObjectSink sinkTask = new DataObjectSink();
    trainingBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        sourceTask, dataStreamerParallelism);
    ComputeConnection firstGraphComputeConnection = trainingBuilder.addSink(
        Constants.SimpleGraphConfig.DATA_OBJECT_SINK, sinkTask, dataStreamerParallelism);
    firstGraphComputeConnection.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE,
        Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
    trainingBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph = trainingBuilder.build();
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, Constants.SimpleGraphConfig.DATA_OBJECT_SINK);
    LOG.info("Training Data Total Partitions : " + dataPointsObject.getPartitions().length);


    InputDataStreamer dataStreamer = new InputDataStreamer(this.operationMode,
        svmJobParameters.isDummy(), this.binaryBatchModel);
    SVMCompute svmCompute = new SVMCompute(this.binaryBatchModel, this.operationMode);
    SVMReduce svmReduce = new SVMReduce(this.operationMode);

    trainingBuilder.addSource(Constants.SimpleGraphConfig.DATASTREAMER_SOURCE, dataStreamer,
        dataStreamerParallelism);
    ComputeConnection svmComputeConnection = trainingBuilder
        .addCompute(Constants.SimpleGraphConfig.SVM_COMPUTE, svmCompute, svmComputeParallelism);
    ComputeConnection svmReduceConnection = trainingBuilder
        .addSink(Constants.SimpleGraphConfig.SVM_REDUCE, svmReduce, reduceParallelism);

    svmComputeConnection
        .direct(Constants.SimpleGraphConfig.DATASTREAMER_SOURCE,
            Constants.SimpleGraphConfig.DATA_EDGE, DataType.OBJECT);
    svmReduceConnection
        .reduce(Constants.SimpleGraphConfig.SVM_COMPUTE, Constants.SimpleGraphConfig.REDUCE_EDGE,
            new ReduceAggregator(), DataType.OBJECT);

    trainingBuilder.setMode(operationMode);
    DataFlowTaskGraph graph = trainingBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);

    taskExecutor.addInput(
        graph, plan, Constants.SimpleGraphConfig.DATASTREAMER_SOURCE,
        Constants.SimpleGraphConfig.INPUT_DATA, dataPointsObject);

    taskExecutor.execute(graph, plan);

    LOG.info("Task Graph Executed !!! ");

    if (operationMode.equals(OperationMode.BATCH)) {
      DataObject<double[]> dataSet = taskExecutor.getOutput(graph, plan,
          Constants.SimpleGraphConfig.SVM_REDUCE);
      LOG.info(String.format("Number of Partitions : %d ", dataSet.getPartitions().length));
      DataPartition<double[]> values = dataSet.getPartitions()[0];
      DataPartitionConsumer<double[]> dataPartitionConsumer = values.getConsumer();
      //LOG.info("Final Receive  : " + dataPartitionConsumer.hasNext());
      while (dataPartitionConsumer.hasNext()) {
        LOG.info("Final Aggregated Values Are:"
            + Arrays.toString(dataPartitionConsumer.next()));
      }

      /**
       * Prediction Task
       * */
      TaskGraphBuilder testBuilder = TaskGraphBuilder.newBuilder(config);
      final String TEST_DATA_LOAD_EDGE_DIRECT = "direct2";
      //TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
      DataObjectSource sourceTask1 = new DataObjectSource(TEST_DATA_LOAD_EDGE_DIRECT,
          this.svmJobParameters.getTestingDataDir());
      DataObjectSink sinkTask1 = new DataObjectSink();
      testBuilder.addSource(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
          sourceTask1, dataStreamerParallelism);
      ComputeConnection firstGraphComputeConnection1 = testBuilder.addSink(
          Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING, sinkTask1, dataStreamerParallelism);
      firstGraphComputeConnection1.direct(Constants.SimpleGraphConfig.DATA_OBJECT_SOURCE_TESTING,
          TEST_DATA_LOAD_EDGE_DIRECT, DataType.OBJECT);
      testBuilder.setMode(OperationMode.BATCH);

      DataFlowTaskGraph datapointsTaskGraph1 = testBuilder.build();
      ExecutionPlan firstGraphExecutionPlan1 = taskExecutor.plan(datapointsTaskGraph1);
      taskExecutor.execute(datapointsTaskGraph1, firstGraphExecutionPlan1);
      DataObject<Object> dataPointsObject1 = taskExecutor.getOutput(
          datapointsTaskGraph1, firstGraphExecutionPlan1,
          Constants.SimpleGraphConfig.DATA_OBJECT_SINK_TESTING);
      LOG.info("Testing Data Total Partitions : " + dataPointsObject1.getPartitions().length);


      PredictionSourceTask predictionSourceTask
          = new PredictionSourceTask(svmJobParameters.isDummy(), this.binaryBatchModel,
          operationMode);
      PredictionReduceTask predictionReduceTask = new PredictionReduceTask(operationMode);

      testBuilder.addSource(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
          predictionSourceTask,
          dataStreamerParallelism);
      ComputeConnection predictionReduceConnection = testBuilder
          .addSink(Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK, predictionReduceTask,
              reduceParallelism);
      predictionReduceConnection
          .reduce(Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
              Constants.SimpleGraphConfig.PREDICTION_EDGE, new PredictionAggregator(),
              DataType.OBJECT);
      testBuilder.setMode(operationMode);
      DataFlowTaskGraph predictionGraph = testBuilder.build();
      ExecutionPlan predictionPlan = taskExecutor.plan(predictionGraph);
      // adding test data set
      taskExecutor
          .addInput(predictionGraph, predictionPlan,
              Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
              Constants.SimpleGraphConfig.TEST_DATA, dataPointsObject1);
      // adding final weight vector
      taskExecutor
          .addInput(predictionGraph, predictionPlan,
              Constants.SimpleGraphConfig.PREDICTION_SOURCE_TASK,
              Constants.SimpleGraphConfig.FINAL_WEIGHT_VECTOR, dataSet);

      taskExecutor.execute(predictionGraph, predictionPlan);
      DataObject<Object> finalRes = taskExecutor.getOutput(predictionGraph, predictionPlan,
          Constants.SimpleGraphConfig.PREDICTION_REDUCE_TASK);
      Object o = finalRes.getPartitions()[0].getConsumer().next();
      if (o instanceof Double) {
        LOG.info(String.format("Testing Accuracy  : %f ", (double) o));
      } else {
        LOG.severe("Something Went Wrong !!!");
      }


    }
  }
}
