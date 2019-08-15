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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeAccuracyReduceFunction;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMAccuracyReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.IterativeSVMWeightVectorReduce;
import edu.iu.dsc.tws.examples.ml.svm.aggregate.ReduceAggregator;
import edu.iu.dsc.tws.examples.ml.svm.compute.IterativeStreamingCompute;
import edu.iu.dsc.tws.examples.ml.svm.compute.window.IterativeStreamingSinkEvaluator;
import edu.iu.dsc.tws.examples.ml.svm.compute.window.IterativeStreamingWindowedCompute;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.constant.IterativeSVMConstants;
import edu.iu.dsc.tws.examples.ml.svm.constant.TimingConstants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeDataStream;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativePredictionDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.streamer.IterativeStreamingDataStreamer;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TGUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.TrainedModel;
import edu.iu.dsc.tws.examples.ml.svm.util.WindowArguments;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.constant.WindowType;
import edu.iu.dsc.tws.task.window.core.BaseWindowedSink;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

public class SvmSgdOnlineRunner extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SvmSgdOnlineRunner.class.getName());

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
  private IterativeStreamingDataStreamer iterativeStreamingDataStreamer;
  private IterativeStreamingCompute iterativeStreamingCompute;
  private IterativeStreamingWindowedCompute iterativeStreamingWindowedCompute;
  private IterativePredictionDataStreamer iterativePredictionDataStreamer;
  private IterativeSVMAccuracyReduce iterativeSVMAccuracyReduce;
  private IterativeSVMWeightVectorReduce iterativeSVMRiterativeSVMWeightVectorReduce;
  private DataObject<double[][]> trainingDoubleDataPointObject;
  private DataObject<double[][]> testingDoubleDataPointObject;
  private DataObject<double[]> inputDoubleWeightvectorObject;
  private DataObject<double[]> currentDataPoint;
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
  private static int count = 0;
  private String experimentName = "";

  @Override
  public void execute() {
    // method 2
    this.initialize()
        .paramCheck()
        .loadData()
        .stream()
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

  public SvmSgdOnlineRunner stream() {
    this.withWeightVector();
    streamData();
    return this;
  }

  public SvmSgdOnlineRunner paramCheck() {
    LOG.info(String.format("Info : %s ", this.svmJobParameters.toString()));
    return this;
  }


  private void loadTrainingData() {
    ComputeGraph trainingDFTG = TGUtils
        .buildTrainingDataPointsTG(this.dataStreamerParallelism,
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
    ComputeGraph testingDFTG = TGUtils
        .buildTestingDataPointsTG(this.dataStreamerParallelism,
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
          .nextInt(this.svmJobParameters.getTestingSamples() / dataStreamerParallelism - 1);
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
    save();
  }

  private void save() {
    TrainedModel trainedModel = new TrainedModel(this.binaryBatchModel, accuracy, this.trainingTime,
        this.svmJobParameters.getExperimentName() + "-online", this.svmJobParameters
        .getParallelism());
    trainedModel.saveModel(this.svmJobParameters.getModelSaveDir());
  }

  private void convert2Seconds() {
    this.initializingDTime = this.initializingTime / TimingConstants.NANO_TO_SEC;
    this.dataLoadingDTime = this.dataLoadingTime / TimingConstants.NANO_TO_SEC;
    this.trainingDTime = this.trainingTime / TimingConstants.NANO_TO_SEC;
    this.testingDTime = this.testingTime / TimingConstants.NANO_TO_SEC;
  }

  private void streamData() {
    ComputeGraph streamingTrainingTG = buildStreamingTrainingTG();
    ExecutionPlan executionPlan = taskExecutor.plan(streamingTrainingTG);
    taskExecutor.addInput(
        streamingTrainingTG, executionPlan,
        Constants.SimpleGraphConfig.ITERATIVE_STREAMING_DATASTREAMER_SOURCE,
        Constants.SimpleGraphConfig.INPUT_DATA, trainingDoubleDataPointObject);
    taskExecutor.addInput(streamingTrainingTG, executionPlan,
        Constants.SimpleGraphConfig.ITERATIVE_STREAMING_DATASTREAMER_SOURCE,
        Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, inputDoubleWeightvectorObject);
    taskExecutor.addInput(streamingTrainingTG, executionPlan,
        "window-sink",
        Constants.SimpleGraphConfig.TEST_DATA, testingDoubleDataPointObject);

    taskExecutor.execute(streamingTrainingTG, executionPlan);
//    currentDataPoint = taskExecutor.getOutput(streamingTrainingTG,
//        executionPlan,
//        Constants.SimpleGraphConfig.ITERATIVE_STREAMING_SVM_COMPUTE);
//    double[] dp = currentDataPoint.getPartitions(0).getConsumer().next();
//    LOG.info(String.format("DataPoint[%d] : %s", count++, Arrays.toString(dp)));
  }

  private ComputeGraph buildStreamingTrainingTG() {
    iterativeStreamingDataStreamer = new IterativeStreamingDataStreamer(this.svmJobParameters
        .getFeatures(), OperationMode.STREAMING, this.svmJobParameters.isDummy(),
        this.binaryBatchModel);
    BaseWindowedSink baseWindowedSink = getWindowSinkInstance();
    iterativeStreamingCompute = new IterativeStreamingCompute(OperationMode.STREAMING,
        new ReduceAggregator(), this.svmJobParameters);

    IterativeStreamingSinkEvaluator iterativeStreamingSinkEvaluator
        = new IterativeStreamingSinkEvaluator();

    trainingBuilder.addSource(Constants.SimpleGraphConfig.ITERATIVE_STREAMING_DATASTREAMER_SOURCE,
        iterativeStreamingDataStreamer, dataStreamerParallelism);
    ComputeConnection svmComputeConnection = trainingBuilder
        .addCompute(Constants.SimpleGraphConfig.ITERATIVE_STREAMING_SVM_COMPUTE,
            baseWindowedSink,
            dataStreamerParallelism);

    ComputeConnection svmReduceConnection = trainingBuilder
        .addCompute("window-sink", iterativeStreamingCompute, dataStreamerParallelism);

    ComputeConnection svmFinalEvaluationConnection = trainingBuilder
        .addSink("window-evaluation-sink", iterativeStreamingSinkEvaluator,
            dataStreamerParallelism);

    svmComputeConnection.direct(Constants.SimpleGraphConfig
        .ITERATIVE_STREAMING_DATASTREAMER_SOURCE)
        .viaEdge(Constants.SimpleGraphConfig.STREAMING_EDGE)
        .withDataType(MessageTypes.DOUBLE_ARRAY);

    svmReduceConnection
        .allreduce(Constants.SimpleGraphConfig.ITERATIVE_STREAMING_SVM_COMPUTE)
        .viaEdge("window-sink-edge")
        .withReductionFunction(new ReduceAggregator())
        .withDataType(MessageTypes.DOUBLE_ARRAY);

    svmFinalEvaluationConnection
        .allreduce("window-sink")
        .viaEdge("window-evaluation-edge")
        .withReductionFunction(new IterativeAccuracyReduceFunction())
        .withDataType(MessageTypes.DOUBLE);

    trainingBuilder.setMode(OperationMode.STREAMING);
    trainingBuilder.setTaskGraphName(IterativeSVMConstants.ITERATIVE_STREAMING_TRAINING_TASK_GRAPH);
    return trainingBuilder.build();
  }

  private BaseWindowedSink getWindowSinkInstance() {
    BaseWindowedSink baseWindowedSink = new IterativeStreamingWindowedCompute(
        new ProcessWindowFunctionImpl(),
        OperationMode.STREAMING,
        this.svmJobParameters,
        this.binaryBatchModel,
        "online-training-graph");
    WindowArguments windowArguments = this.svmJobParameters.getWindowArguments();
    TimeUnit timeUnit = TimeUnit.MICROSECONDS;
    if (windowArguments != null) {
      WindowType windowType = windowArguments.getWindowType();
      if (windowArguments.isDuration()) {
        if (windowType.equals(WindowType.TUMBLING)) {
          baseWindowedSink
              .withTumblingDurationWindow(windowArguments.getWindowLength(), timeUnit);
        }
        if (windowType.equals(WindowType.SLIDING)) {
          baseWindowedSink
              .withSlidingDurationWindow(windowArguments.getWindowLength(), timeUnit,
                  windowArguments.getSlidingLength(), timeUnit);
        }
      } else {
        if (windowType.equals(WindowType.TUMBLING)) {
          baseWindowedSink
              .withTumblingCountWindow(windowArguments.getWindowLength());
        }
        if (windowType.equals(WindowType.SLIDING)) {
          baseWindowedSink
              .withSlidingCountWindow(windowArguments.getWindowLength(),
                  windowArguments.getSlidingLength());
        }
      }
    }
    return baseWindowedSink;
  }

  protected static class ProcessWindowFunctionImpl implements ProcessWindowedFunction<double[]> {

    private static final long serialVersionUID = 8517840191276879034L;

    private static final Logger LOG = Logger.getLogger(ProcessWindowFunctionImpl.class.getName());

    @Override
    public IWindowMessage<double[]> process(IWindowMessage<double[]> windowMessage) {
      return windowMessage;
    }

    @Override
    public IMessage<double[]> processLateMessage(IMessage<double[]> lateMessage) {
      return lateMessage;
    }

    @Override
    public double[] onMessage(double[] object1, double[] object2) {
      try {
        return Matrix.add(object1, object2);
      } catch (MatrixMultiplicationException e) {
        LOG.severe(String.format("Math Error : %s", e.getMessage()));
      }
      return null;
    }
  }

}
