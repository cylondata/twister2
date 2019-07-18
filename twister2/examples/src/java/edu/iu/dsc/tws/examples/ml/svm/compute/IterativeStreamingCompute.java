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
package edu.iu.dsc.tws.examples.ml.svm.compute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.nodes.BaseCompute;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.ICollector;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IReceptor;
import edu.iu.dsc.tws.examples.ml.svm.math.Matrix;
import edu.iu.dsc.tws.examples.ml.svm.util.MLUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TrainedModel;

public class IterativeStreamingCompute extends BaseCompute<double[]>
    implements ICollector<double[]>, IReceptor<double[][]> {
  private static final long serialVersionUID = 332173590941256461L;
  private static final Logger LOG = Logger.getLogger(IterativeStreamingCompute.class.getName());
  private List<double[]> aggregatedModels = new ArrayList<>();

  private double[] newWeightVector;

  private boolean debug = false;

  private OperationMode operationMode;

  private IFunction<double[]> reduceFn;

  private int evaluationInterval = 10;

  private DataObject<double[][]> dataPointsObject = null;

  private double[][] datapoints = null;

  private SVMJobParameters svmJobParameters;

  private TrainedModel trainedModel;


  public IterativeStreamingCompute(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public IterativeStreamingCompute(OperationMode operationMode, IFunction<double[]> reduceFn) {
    this.operationMode = operationMode;
    this.reduceFn = reduceFn;
  }

  public IterativeStreamingCompute(OperationMode operationMode, IFunction<double[]> reduceFn,
                                   SVMJobParameters svmJobParameters) {
    this.operationMode = operationMode;
    this.reduceFn = reduceFn;
    this.svmJobParameters = svmJobParameters;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    prepareDataPoints();
    LOG.info(String.format("Test Data Size : %d ", this.datapoints.length));
  }

  @Override
  public DataPartition<double[]> get() {
    return new EntityPartition<>(context.taskIndex(), newWeightVector);
  }

  @Override
  public void add(String name, DataObject<?> data) {
    if (Constants.SimpleGraphConfig.TEST_DATA.equals(name)) {
      this.dataPointsObject = (DataObject<double[][]>) data;
    }
  }

  private void prepareDataPoints() {
    DataPartition<double[][]> dataPartition = this.dataPointsObject
        .getPartition(context.taskIndex());
    this.datapoints = dataPartition.getConsumer().next();
    if (debug) {
      LOG.info(String.format("Recieved Input Data : %s ", this.datapoints.getClass().getName()));
    }
    LOG.info(String.format("Data Point TaskIndex[%d], Size : %d ", context.taskIndex(),
        this.datapoints.length));
  }


  @Override
  public boolean execute(IMessage<double[]> message) {
    if (message.getContent() == null) {
      LOG.info("Something Went Wrong !!!");
    } else {
      if (debug) {
        LOG.info(String.format("Received Sink Value : %d, %s, %f", this.newWeightVector.length,
            Arrays.toString(this.newWeightVector), this.newWeightVector[0]));
      }
      this.newWeightVector = message.getContent();
      aggregatedModels.add(this.newWeightVector);
      double[] w = new double[this.aggregatedModels.get(0).length];
      int size = aggregatedModels.size();
      for (int i = 0; i < size; i++) {
        w = Matrix.scalarDivide(reduceFn.onMessage(w, aggregatedModels.get(i)), size);
      }
      evaluateModel(w, size);
    }
    return true;
  }

  public void evaluateModel(double[] w, int evalCount) {
    try {
      trainedModel = MLUtils.predictSGDSVM(w, this.datapoints, this.svmJobParameters,
          "final-model");
    } catch (MatrixMultiplicationException e) {
      LOG.severe(String.format("MatrixMultiplicationException : " + e.getMessage()));
    }
    if (debug) {
      LOG.info(String.format("Evaluation TimeStamp [%d] Model : %s, Accuracy : %f", evalCount,
          Arrays.toString(w), trainedModel.getAccuracy()));
    }
    context.write("window-evaluation-edge", trainedModel.getAccuracy());
  }


}
