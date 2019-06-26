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
package edu.iu.dsc.tws.examples.ml.svm.streamer;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.nodes.BaseSource;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.InputDataFormatException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IReceptor;
import edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos.PegasosSgdSvm;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;

public class IterativeDataStream extends BaseSource implements IReceptor<double[][]> {
  private static final Logger LOG = Logger.getLogger(IterativeDataStream.class.getName());
  private static final long serialVersionUID = 6672551932831677547L;

  private final double[] labels = {-1, +1};
  private int features = 10;
  private OperationMode operationMode;

  private boolean isDummy = false;

  private BinaryBatchModel binaryBatchModel;

  private DataObject<double[][]> dataPointsObject = null;

  private DataObject<double[]> weightVectorObject = null;

  private double[][] datapoints = null;

  private double[] weightVector = null;

  private double[] computedWeightVector = null;

  private PegasosSgdSvm pegasosSgdSvm = null;

  private boolean debug = false;

  public IterativeDataStream(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public IterativeDataStream(int features, OperationMode operationMode) {
    this.features = features;
    this.operationMode = operationMode;
  }

  public IterativeDataStream(int features, OperationMode operationMode, boolean isDummy,
                             BinaryBatchModel binaryBatchModel) {
    this.features = features;
    this.operationMode = operationMode;
    this.isDummy = isDummy;
    this.binaryBatchModel = binaryBatchModel;
  }


  @Override
  public void add(String name, DataObject<?> data) {
    if (debug) {
      LOG.log(Level.INFO, String.format("Received input: %s ", name));
    }

    if (Constants.SimpleGraphConfig.INPUT_DATA.equals(name)) {
      this.dataPointsObject = (DataObject<double[][]>) data;
    }

    if (Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR.equals(name)) {
      this.weightVectorObject = (DataObject<double[]>) data;
    }
  }

  @Override
  public void execute() {
    if (this.isDummy) {
      try {
        dummyDataStreamer();
      } catch (InputDataFormatException e) {
        e.printStackTrace();
      }
    } else {
      realDataStreamer();
    }
  }

  public void getData() {
    DataPartition<double[][]> dataPartition = dataPointsObject.getPartitions(context.taskIndex());
    this.datapoints = dataPartition.getConsumer().next();
    DataPartition<double[]> weightVectorPartition = weightVectorObject.getPartitions(context
        .taskIndex());
    this.weightVector = weightVectorPartition.getConsumer().next();

    if (debug) {
      LOG.info(String.format("Recieved Input Data : %s ", this.datapoints.getClass().getName()));
    }


    LOG.info(String.format("Data Point TaskIndex[%d], Size : %d ", context.taskIndex(),
        this.datapoints.length));
    LOG.info(String.format("Weight Vector TaskIndex[%d], Size : %d ", context.taskIndex(),
        weightVector.length));
    //LOG.info(String.format("Data Points : %s", Arrays.deepToString(this.datapointArray)));
  }

  /**
   * This method is used to deal with dummy data based data stream generation
   * Here data points are generated using a Gaussian Distribution and labels are assigned
   * +1 or -1 randomly for a given data point.
   */
  public void dummyDataStreamer() throws InputDataFormatException {
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      double[] x = DataUtils.seedDoubleArray(this.binaryBatchModel.getFeatures());
      Random random = new Random();
      int index = random.nextInt(2);
      double label = labels[index];
      double[] data = DataUtils.combineLabelAndData(x, label);
      if (!(data.length == this.binaryBatchModel.getFeatures() + 1)) {
        throw
            new InputDataFormatException(String
                .format("Input Data Format Exception : [data length : %d, feature length +1 : %d]",
                    data.length, this.binaryBatchModel.getFeatures() + 1));
      }
      this.context.write(Constants.SimpleGraphConfig.DATA_EDGE, data);
    }

    if (this.operationMode.equals(OperationMode.BATCH)) {
      double[][] data = DataUtils.generateDummyDataPoints(this.binaryBatchModel.getSamples(),
          this.binaryBatchModel.getFeatures());
      this.context.write(Constants.SimpleGraphConfig.DATA_EDGE, data);
      this.context.end(Constants.SimpleGraphConfig.DATA_EDGE);
    }

  }

  /**
   * This method is used to retrieve real data from
   * Data Source Task
   */
  public void realDataStreamer() {
    // do real data streaming
    if (this.operationMode.equals(OperationMode.BATCH)) {
      getData();
      initializeBatchMode();
      compute();
      this.context.writeEnd(Constants.SimpleGraphConfig.REDUCE_EDGE, computedWeightVector);
    } else {
      LOG.info(String.format("Real data stream got stuck!"));
    }

  }

  public void compute() {
    double[][] x = this.binaryBatchModel.getX();
    double[] w = this.binaryBatchModel.getW();
    double[] y = this.binaryBatchModel.getY();
    try {
      pegasosSgdSvm.iterativeTaskSgd(w, x, y);
    } catch (NullDataSetException e) {
      e.printStackTrace();
    } catch (MatrixMultiplicationException e) {
      e.printStackTrace();
    }
    computedWeightVector = DataUtils.average(pegasosSgdSvm.getW(), context.getParallelism());
  }

  /**
   * This method initializes the parameters needed to run the batch mode algorithm
   * This method is called per a received batch
   */
  public void initializeBatchMode() {
    this.initializeBinaryModel(this.datapoints);
    this.binaryBatchModel.setW(this.weightVector);
    LOG.info(String.format("Features in X : %d, Features in W : %d",
        this.binaryBatchModel.getFeatures(), this.binaryBatchModel.getW().length));
    pegasosSgdSvm = new PegasosSgdSvm(this.binaryBatchModel.getW(), this.binaryBatchModel.getX(),
        this.binaryBatchModel.getY(), this.binaryBatchModel.getAlpha(),
        this.binaryBatchModel.getIterations(), this.binaryBatchModel.getFeatures());
  }

  /**
   * Binary Model is updated with received batch data
   *
   * @param xy data points included with label and features
   */
  public void initializeBinaryModel(double[][] xy) {
    if (binaryBatchModel == null) {
      throw new NullPointerException("Binary Batch Model is Null !!!");
    }
    if (debug) {
      LOG.info("Binary Batch Model Before Updated : " + this.binaryBatchModel.toString());
    }
    this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel, xy);
    if (debug) {
      LOG.info("Binary Batch Model After Updated : " + this.binaryBatchModel.toString());
      LOG.info(String.format("Updated Data [%d,%d] ",
          this.binaryBatchModel.getX().length, this.binaryBatchModel.getX()[0].length));
    }

  }

}
