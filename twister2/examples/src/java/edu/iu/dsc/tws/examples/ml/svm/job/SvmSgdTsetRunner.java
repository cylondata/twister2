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
import java.util.Arrays;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableMapTSet;
import edu.iu.dsc.tws.examples.ml.svm.constant.TimingConstants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;
import edu.iu.dsc.tws.examples.ml.svm.tset.AccuracyAverager;
import edu.iu.dsc.tws.examples.ml.svm.tset.DataLoadingTask;
import edu.iu.dsc.tws.examples.ml.svm.tset.SvmTestMap;
import edu.iu.dsc.tws.examples.ml.svm.tset.SvmTrainMap;
import edu.iu.dsc.tws.examples.ml.svm.tset.WeightVectorLoad;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.IPrintFunction;
import edu.iu.dsc.tws.examples.ml.svm.util.ResultsSaver;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TSetUtils;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SvmSgdTsetRunner extends TSetBatchWorker implements Serializable {

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
  private double dataLoadingTime = 0;
  private double trainingTime = 0;
  private double testingTime = 0;
  private double accuracy = 0;
  private boolean debug = false;
  private String experimentName = "";
  private TwisterBatchContext twisterBatchContext;

  private boolean testStatus = false;


  @Override
  public void execute(TwisterBatchContext tc) {
    this.twisterBatchContext = tc;
    initializeParameters();
    trainingData = loadTrainingData();
    testingData = loadTestingData();
    trainedWeightVector = loadWeightVector();
    TSetUtils.printCachedTset(trainedWeightVector, new IPrintFunction<double[]>() {
      @Override
      public void print(double[] doubles) {
        System.out.println(Arrays.toString(doubles));
      }
    });

  }

  /**
   * This method initializes the parameters in running SVM
   */
  private void initializeParameters() {
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
  }

  private CachedTSet<double[][]> loadTrainingData() {
    CachedTSet<double[][]> data = this.twisterBatchContext.createSource(
        new DataLoadingTask(this.binaryBatchModel, this.svmJobParameters, "train"),
        this.dataStreamerParallelism).setName("trainingDataSource").cache();
    return data;
  }

  private CachedTSet<double[][]> loadTestingData() {
    CachedTSet<double[][]> data = this.twisterBatchContext.createSource(
        new DataLoadingTask(this.binaryBatchModel, this.svmJobParameters, "test"),
        this.dataStreamerParallelism).setName("testingDataSource").cache();
    return data;
  }

  private CachedTSet<double[]> loadWeightVector() {
    CachedTSet<double[]> weightVector = this.twisterBatchContext.createSource(
        new WeightVectorLoad(this.binaryBatchModel, this.svmJobParameters),
        this.dataStreamerParallelism).setName("weightVectorSource")
        .cache();
    return weightVector;
  }

  private void executeTrain() {
    long time = System.nanoTime();
//    for (int i = 0; i < this.svmJobParameters.getIterations(); i++) {
//
//    }
    IterableMapTSet<double[][], double[]> svmTrainTset = trainingData
        .map(new SvmTrainMap(this.binaryBatchModel, this.svmJobParameters));

    AllReduceTLink<double[]> allReduceTLink = svmTrainTset.allReduce((t1, t2) -> {
      double[] w1 = new double[t1.length];
      try {
        w1 = Matrix.add(t1, t2);
      } catch (MatrixMultiplicationException e) {
        e.printStackTrace();
      }
      return w1;
    });
    // sink must be there to execute the Map task
    allReduceTLink.sink(value -> {
      return true;
    }, this.svmJobParameters.getParallelism());


    this.trainingTime = ((double) (System.nanoTime() - time)) / TimingConstants.NANO_TO_SEC;
//    LOG.info(String.format("Data Point TaskIndex[%d], Size : %d => Array Size : [%d,%d]",
//        this.workerId,
//        trainingData.getData().size(), trainingData.getData().get(0).length,
//        trainingData.getData().get(0)[0].length));
    // TODO :: Handle the worker 0 senario for getOutput
    LOG.info(String.format("Rank[%d][%d] Total Memory %f MB, Max Memory %f MB, Training Completed! "
            + "=> Data Loading Time %f , Training Time : %f ", workerId,
        trainingData.getData().get(0).length,
        ((double) Runtime.getRuntime().totalMemory()) / TimingConstants.B2MB,
        ((double) Runtime.getRuntime().maxMemory()) / TimingConstants.B2MB, dataLoadingTime,
        trainingTime));
  }

  private void executeSummary() {
    if (workerId == 0) {
//      CachedTSet<double[]> finalW = reduceTLink
//          .map(new ModelAverager(this.svmJobParameters.getParallelism())).cache();
//      double[] wFinal = finalW.getData().get(0);
//      this.binaryBatchModel.setW(wFinal);
//
//      LOG.info(String.format("Data : %s", Arrays.toString(wFinal)));
      try {
        saveResults();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void executePredict() {
    if (testStatus) {
      IterableMapTSet<double[][], Double> svmTestTset = testingData
          .map(new SvmTestMap(this.binaryBatchModel, this.svmJobParameters));
      ReduceTLink<Double> reduceTestLink = svmTestTset.reduce((t1, t2) -> {
        double t = t1 + t2;
        return t;
      });
      CachedTSet<Double> finalAcc = reduceTestLink
          .map(new AccuracyAverager(this.svmJobParameters.getParallelism())).cache();
      double acc = finalAcc.getData().get(0);
      LOG.info(String.format("Training Accuracy : %f ", acc));
    }
  }


  private SvmSgdTsetRunner train() {

    return this;
  }

  private SvmSgdTsetRunner predict() {

    return this;
  }

  private SvmSgdTsetRunner summary() {

    return this;
  }


  private SvmSgdTsetRunner loadData() {
    trainingData = loadTrainingData();
    testingData = loadTestingData();
    trainedWeightVector = loadWeightVector();
    return this;
  }


  private void saveResults() throws IOException {
    ResultsSaver resultsSaver = new ResultsSaver(this.trainingTime, this.testingTime,
        this.dataLoadingTime, this.dataLoadingTime + this.trainingTime + this.testingTime,
        this.svmJobParameters, "tset");
    resultsSaver.save();
  }

  private void executeAll() {
    this
        .loadData()
        .train()
        .predict()
        .summary();
  }
}
