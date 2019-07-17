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

import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.worker.TSetIWorker;
import edu.iu.dsc.tws.examples.ml.svm.constant.TimingConstants;
import edu.iu.dsc.tws.examples.ml.svm.tset.DataLoadingTask;
import edu.iu.dsc.tws.examples.ml.svm.tset.WeightVectorLoad;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.ResultsSaver;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;


public class SvmSgdTsetRunner implements TSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(SvmSgdTsetRunner.class.getName());

  private final int reduceParallelism = 1;
  private int dataStreamerParallelism = 4;
  private int svmComputeParallelism = 4;
  private int features = 10;
  private OperationMode operationMode;
  private SVMJobParameters svmJobParameters;
  private BinaryBatchModel binaryBatchModel;
  private CachedTSet<double[]> trainedWeightVector;
  private CachedTSet<double[][]> trainingData;
  private CachedTSet<double[][]> testingData;
  private long dataLoadingTime = 0L;
  private long initializingTime = 0L;
  private double initializingDTime = 0;
  private long trainingTime = 0L;
  private long testingTime = 0L;
  private double dataLoadingDTime = 0L;
  private double trainingDTime = 0L;
  private double testingDTime = 0L;
  private double totalTime = 0;
  private double accuracy = 0;
  private boolean debug = false;
  private String experimentName = "";
//  private TwisterBatchContext twisterBatchContext;

  private boolean testStatus = false;

  private void executeAll(TSetEnvironment env) {
    this
        .initialize(env)
        .loadData(env)
        .train(env)
        .predict(env)
        .summary(env)
        .save(env);
  }

  @Override
  public void execute(TSetEnvironment env) {
//    Method 1
//    initializeParameters();
//    trainingData = loadTrainingData();
//    testingData = loadTestingData();
//    trainedWeightVector = loadWeightVector();
//    TSetUtils.printCachedTset(trainedWeightVector,
//        doubles -> System.out.println(Arrays.toString(doubles)));
//    executeTraining();
//    executePredict();

//    Method 2
    executeAll(env);

  }

  @Override
  public OperationMode getOperationMode() {
    return OperationMode.BATCH;
  }

  /**
   * This method initializes the parameters in running SVM
   */
  private void initializeParameters(TSetEnvironment env) {
    this.svmJobParameters = SVMJobParameters.build(env.getConfig());
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
  }

  private CachedTSet<double[][]> loadTrainingData(TSetEnvironment env) {
    CachedTSet<double[][]> data = env.createBatchSource(
        new DataLoadingTask(this.binaryBatchModel, this.svmJobParameters, "train"),
        this.dataStreamerParallelism).setName("trainingDataSource").cache();
    return data;
  }

  private CachedTSet<double[][]> loadTestingData(TSetEnvironment env) {
    CachedTSet<double[][]> data = env.createBatchSource(
        new DataLoadingTask(this.binaryBatchModel, this.svmJobParameters, "test"),
        this.dataStreamerParallelism).setName("testingDataSource").cache();
    return data;
  }

  private CachedTSet<double[]> loadWeightVector(TSetEnvironment env) {
    CachedTSet<double[]> weightVector = env.createBatchSource(
        new WeightVectorLoad(this.binaryBatchModel, this.svmJobParameters),
        this.dataStreamerParallelism).setName("weightVectorSource")
        .cache();
    return weightVector;
  }

  private void executeTraining(TSetEnvironment env) {
/*    long time = System.nanoTime();
    this.binaryBatchModel.setW(this.trainedWeightVector.getPartitionData(0));
    for (int i = 0; i < this.svmJobParameters.getIterations(); i++) {
      LOG.info(String.format("Iteration %d", i));
      IterableMapTSet<double[][], double[]> svmTrainTset = trainingData
          .map(new SvmTrainMap(this.binaryBatchModel, this.svmJobParameters));
      svmTrainTset.addInput(Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR, trainedWeightVector);
      AllReduceTLink<double[]> reduceTLink = svmTrainTset.allReduce((t1, t2) -> {
        double[] newWeightVector = new double[t1.length];
        try {
          newWeightVector = Matrix.add(t1, t2);
        } catch (MatrixMultiplicationException e) {
          e.printStackTrace();
        }
        return newWeightVector;
      });
      trainedWeightVector = reduceTLink
          .map(new WeightVectorAverager(this.dataStreamerParallelism),
              this.dataStreamerParallelism)
          .cache();
      //TODO : Think
      // TDirectLink is not serializable or any of the super classes are not serializable
      // so this is hard to do without that support. Config class is also not serializable
//      trainedWeightVector = reduceTLink
//          .map((MapFunction<double[], double[]> & Serializable)
//              doubles -> Matrix.scalarDivide(doubles, (double) dataStreamerParallelism),
//              dataStreamerParallelism)
//          .cache();
    }
    this.trainingTime = System.nanoTime() - time;
    TSetUtils.printCachedTset(trainedWeightVector, new IPrintFunction<double[]>() {
      @Override
      public void print(double[] doubles) {
        System.out.println(Arrays.toString(doubles));
      }
    });*/
  }

  private void executeSummary(TSetEnvironment env) {
    if (env.getWorkerID() == 0) {
      generateSummary();
    }
  }

  private void executePredict(TSetEnvironment env) {
/*    assert this.trainedWeightVector.getPartitionData(0) != null : "Partition is null";
    this.binaryBatchModel.setW(this.trainedWeightVector.getData().get(0));
    IterableMapTSet<double[][], Double> svmTestTset = testingData
        .map(new SvmTestMap(this.binaryBatchModel, this.svmJobParameters));
    ReduceTLink<Double> reduceTestLink = svmTestTset.reduce((t1, t2) -> {
      double t = t1 + t2;
      return t;
    });
    CachedTSet<Double> finalAcc = reduceTestLink
        .map(new AccuracyAverager(this.svmJobParameters.getParallelism())).cache();
    accuracy = finalAcc.getData().get(0);
    LOG.info(String.format("Training Accuracy : %f ", accuracy));*/
  }

  private SvmSgdTsetRunner initialize(TSetEnvironment env) {
    long t1 = System.nanoTime();
    initializeParameters(env);
    this.initializingTime = System.nanoTime() - t1;
    return this;
  }

  private SvmSgdTsetRunner train(TSetEnvironment env) {
    long t1 = System.nanoTime();
    executeTraining(env);
    this.trainingTime = System.nanoTime() - t1;
    return this;
  }

  private SvmSgdTsetRunner predict(TSetEnvironment env) {
    long t1 = System.nanoTime();
    executePredict(env);
    this.testingTime = System.nanoTime() - t1;
    return this;
  }

  private SvmSgdTsetRunner summary(TSetEnvironment env) {
    executeSummary(env);
    return this;
  }


  private SvmSgdTsetRunner loadData(TSetEnvironment env) {
    long t1 = System.nanoTime();
    trainingData = loadTrainingData(env);
    testingData = loadTestingData(env);
    trainedWeightVector = loadWeightVector(env);
    this.dataLoadingTime = System.nanoTime() - t1;
    return this;
  }

  private SvmSgdTsetRunner save(TSetEnvironment env) {
    try {
      saveResults(env);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return this;
  }


  private void saveResults(TSetEnvironment env) throws IOException {
    ResultsSaver resultsSaver = new ResultsSaver(this.trainingTime, this.testingTime,
        this.dataLoadingTime, this.dataLoadingTime + this.trainingTime + this.testingTime,
        this.svmJobParameters, "tset");
    resultsSaver.save();
  }

  private void generateSummary() {
    convert2Seconds();
    totalTime = initializingDTime + dataLoadingDTime + trainingDTime + testingDTime;
    double totalMemory = ((double) Runtime.getRuntime().totalMemory()) / TimingConstants.B2MB;
    double maxMemory = ((double) Runtime.getRuntime().totalMemory()) / TimingConstants.B2MB;
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
        + String.format(" %.9f", totalTime) + "  s \n";
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
