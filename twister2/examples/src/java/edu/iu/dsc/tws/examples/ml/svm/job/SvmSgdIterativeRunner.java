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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
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

  public DataFlowTaskGraph builtSvmSgdIterativeTaskGraph(int parallelism, Config conf) {
    IterativeDataStream iterativeDataStream
        = new IterativeDataStream(this.svmJobParameters.getFeatures(),
        this.operationMode, this.svmJobParameters.isDummy(), this.binaryBatchModel);
    svmReduce = new SVMReduce(this.operationMode);

    TaskGraphBuilder svmIterativeTaskGraph = TaskGraphBuilder.newBuilder(conf);

    svmIterativeTaskGraph.addSource(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE,
        iterativeDataStream, parallelism);
    ComputeConnection svmComputeConnection = svmIterativeTaskGraph
        .addSink(Constants.SimpleGraphConfig.SVM_REDUCE, svmReduce, reduceParallelism);

    svmComputeConnection.reduce(Constants.SimpleGraphConfig.ITERATIVE_DATASTREAMER_SOURCE)
        .viaEdge(Constants.SimpleGraphConfig.REDUCE_EDGE)
        .withReductionFunction(new ReduceAggregator())
        .withDataType(DataType.OBJECT);

    svmIterativeTaskGraph.setMode(operationMode);
    svmIterativeTaskGraph.setTaskGraphName("iterative-svm-sgd-taskgraph");

    return  svmIterativeTaskGraph.build();
  }


  @Override
  public void execute() {

  }
}
