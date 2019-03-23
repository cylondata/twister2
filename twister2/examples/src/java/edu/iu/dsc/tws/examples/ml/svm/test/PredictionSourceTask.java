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
package edu.iu.dsc.tws.examples.ml.svm.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class PredictionSourceTask extends BaseSource implements Receptor {

  private static final Logger LOG = Logger.getLogger(PredictionSourceTask.class.getName());

  private boolean isDummy = false;

  private BinaryBatchModel binaryBatchModel;

  private DataObject<?> testDataPointsObject = null;

  private Object testDataPoints = null;

  private double[][] testDatapointArray = null;

  private DataObject<?> weightVectorObject = null;

  private Object weighVector = null;

  private double[] weightVectorArray = null;

  private OperationMode operationMode;

  private double accuracy = 0.0;

  private boolean debug = false;

  public PredictionSourceTask(boolean isDummy, BinaryBatchModel binaryBatchModel,
                              OperationMode operationMode) {
    this.isDummy = isDummy;
    this.binaryBatchModel = binaryBatchModel;
    this.operationMode = operationMode;
  }

  @Override
  public void add(String name, DataObject<?> data) {
    LOG.log(Level.INFO, "Received input: " + name);
    if (Constants.SimpleGraphConfig.TEST_DATA.equals(name)) {
      this.testDataPointsObject = data;
    }

    if (Constants.SimpleGraphConfig.FINAL_WEIGHT_VECTOR.equals(name)) {
      this.weightVectorObject = data;
    }
  }

  @Override
  public void execute() {
    if (this.isDummy) {

      dummyTest();

    } else {
      try {
        realTest();
      } catch (MatrixMultiplicationException e) {
        e.printStackTrace();
      }
    }
  }

  public void dummyTest() {
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      //
    }


    if (this.operationMode.equals(OperationMode.BATCH)) {
      //
    }

  }

  public void realTest() throws MatrixMultiplicationException {
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      //
    }


    if (this.operationMode.equals(OperationMode.BATCH)) {
      getData();
      this.context.write(Constants.SimpleGraphConfig.PREDICTION_EDGE, this.accuracy);
      this.context.end(Constants.SimpleGraphConfig.PREDICTION_EDGE);
    }
  }

  public Object getDataPointsByTaskIndex(int taskIndex) {
    EntityPartition<Object> datapointsEntityPartition
        = (EntityPartition<Object>) testDataPointsObject.getPartitions(taskIndex);
    if (datapointsEntityPartition != null) {
      DataObject<?> dataObject
          = (DataObject<?>) datapointsEntityPartition.getConsumer().next();
      testDataPoints = getDataPointsByDataObject(taskIndex, dataObject);
    }
    return testDataPoints;
  }

  public Object getDataPointsByDataObject(int taskIndex, DataObject<?> datapointsDataObject) {
    Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
        datapointsDataObject.getPartitions(taskIndex).getConsumer().next();
    List<Object> items = new ArrayList<>();
    while (arrayListIterator.hasNext()) {
      Object object = arrayListIterator.next();
      items.add(object);
    }
    return items;
  }


  public Object getWeightVectorByTaskIndex(int taskIndex) {
    Object object = null;
    EntityPartition<Object> datapointsEntityPartition
        = (EntityPartition<Object>) weightVectorObject.getPartitions(taskIndex);
    if (datapointsEntityPartition != null) {
      object = datapointsEntityPartition.getConsumer().next();
    }
    return object;
  }

  public Object getWeightVectorByWeightVectorObject(int taskIndex,
                                                    DataObject<?> datapointsDataObject) {
    Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
        datapointsDataObject.getPartitions(taskIndex).getConsumer().next();
    List<Object> items = new ArrayList<>();
    while (arrayListIterator.hasNext()) {
      Object object = arrayListIterator.next();
      items.add(object);
    }
    return items;
  }


  /**
   * This method must be broken down to smaller parts
   * @throws MatrixMultiplicationException
   */
  public void getData() throws MatrixMultiplicationException {
    this.testDataPoints = getDataPointsByTaskIndex(context.taskIndex());
    if (debug) {
      LOG.info(String.format("Recieved Test Input Data : %s ",
          this.testDataPoints.getClass().getName()));
    }
    this.testDatapointArray = DataUtils.getDataPointsFromDataObject(this.testDataPoints);
    if (debug) {
      LOG.info(String.format("Test Data Point TaskIndex[%d], Size : %d ", context.taskIndex(),
          this.testDatapointArray.length));
    }
    //LOG.info(String.format("Data Points : %s", Arrays.deepToString(this.datapointArray)));
    this.weighVector = getWeightVectorByTaskIndex(context.taskIndex());
    if (this.weighVector instanceof double[]) {
      this.weightVectorArray = (double[]) this.weighVector;
      if (debug) {
        LOG.info(String.format("Weight Vector TaskIndex[%d], Size : %d ", context.taskIndex(),
            this.weightVectorArray.length));
        LOG.info(String.format("Weight Vector : %s", Arrays.toString(this.weightVectorArray)));
      }
      this.binaryBatchModel = new BinaryBatchModel();
      this.binaryBatchModel.setW(this.weightVectorArray);
      this.binaryBatchModel.setFeatures(this.weightVectorArray.length);
      this.binaryBatchModel.setSamples(this.testDatapointArray.length);
      this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel,
          this.testDatapointArray);
      if (debug) {
        int currentSamples = this.binaryBatchModel.getSamples();
        int currentFeatures = this.binaryBatchModel.getFeatures();
        int dataSamples = this.binaryBatchModel.getX().length;
        int lengthOfASample = this.binaryBatchModel.getX()[0].length;
        int lengthOfLabels = this.binaryBatchModel.getY().length;
        LOG.info(String.format("Current Samples %d, Current Features %d, Data Samples %d, "
                + "Length of a Sample %d, Length of Labels %d", currentSamples,
            currentFeatures,
            dataSamples, lengthOfASample, lengthOfLabels));
      }
      Predict predict = new Predict(this.binaryBatchModel.getX(), this.binaryBatchModel.getY(),
          this.weightVectorArray);
      this.accuracy = predict.predict();
      LOG.info(String.format("Task Index[%d] Accuracy [%f]", context.taskIndex(),
          this.accuracy));
    }

    //DataUtils.getWeightVectorFromWeightVectorObject(this.weighVector);

  }

  public void doPrediction() throws MatrixMultiplicationException {

  }


}
