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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.ReduceAggregator;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.SVMReduce;
import edu.iu.dsc.tws.examples.ml.svm.compute.SVMCompute;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.streamer.DataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SvmSgdRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdRunner.class.getName());

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
    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);

    this.operationMode = this.svmJobParameters.isStreaming()
        ? OperationMode.STREAMING : OperationMode.BATCH;

    DataStreamer dataStreamer = new DataStreamer(this.operationMode, svmJobParameters.isDummy(),
        this.binaryBatchModel);
    SVMCompute svmCompute = new SVMCompute(this.binaryBatchModel, this.operationMode);
    SVMReduce svmReduce = new SVMReduce(this.operationMode);

    builder.addSource(Constants.SimpleGraphConfig.DATASTREAMER_SOURCE, dataStreamer,
        dataStreamerParallelism);
    ComputeConnection svmComputeConnection = builder
        .addCompute(Constants.SimpleGraphConfig.SVM_COMPUTE, svmCompute, svmComputeParallelism);
    ComputeConnection svmReduceConnection = builder
        .addSink(Constants.SimpleGraphConfig.SVM_REDUCE, svmReduce, reduceParallelism);

    svmComputeConnection
        .direct(Constants.SimpleGraphConfig.DATASTREAMER_SOURCE,
            Constants.SimpleGraphConfig.DATA_EDGE, DataType.OBJECT);
    svmReduceConnection
        .reduce(Constants.SimpleGraphConfig.SVM_COMPUTE, Constants.SimpleGraphConfig.REDUCE_EDGE,
            new ReduceAggregator(), DataType.OBJECT);

    builder.setMode(operationMode);
    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, plan);

    LOG.info("Task Graph Executed !!! ");

    if (operationMode.equals(OperationMode.BATCH)) {
      DataObject<double[]> dataSet = taskExecutor.getOutput(graph, plan,
          Constants.SimpleGraphConfig.SVM_REDUCE);
      DataPartition<double[]> values = dataSet.getPartitions()[0];
      DataPartitionConsumer<double[]> dataPartitionConsumer = values.getConsumer();
      //LOG.info("Final Receive  : " + dataPartitionConsumer.hasNext());
      while (dataPartitionConsumer.hasNext()) {
        LOG.info("Final Aggregated Values Are:"
            + Arrays.toString(dataPartitionConsumer.next()));
      }
    }
  }
}
