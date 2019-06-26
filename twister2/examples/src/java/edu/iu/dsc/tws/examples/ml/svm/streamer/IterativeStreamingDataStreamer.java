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

import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.InputDataFormatException;
import edu.iu.dsc.tws.examples.ml.svm.integration.test.IReceptor;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class IterativeStreamingDataStreamer extends BaseSource implements IReceptor<double[]> {
  private static final long serialVersionUID = -5845248990437663713L;
  private static final Logger LOG = Logger.getLogger(IterativeStreamingDataStreamer.class
      .getName());

  private final double[] labels = {-1, +1};
  private int features = 10;
  private OperationMode operationMode;

  private boolean isDummy = false;

  private BinaryBatchModel binaryBatchModel;

  private DataObject<double[][]> dataPointsObject = null;

  private DataObject<double[]> weightVectorObject = null;

  private double[][] datapoints = null;

  private double[] weightVector = null;

  private boolean debug = false;


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

  public IterativeStreamingDataStreamer(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public IterativeStreamingDataStreamer(int features, OperationMode operationMode) {
    this.features = features;
    this.operationMode = operationMode;
  }

  public IterativeStreamingDataStreamer(int features, OperationMode operationMode, boolean isDummy,
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
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      getData();
      streamData();
    } else {
      LOG.info(String.format("This Data Source only supports for streaming tasks"));
    }
  }

  public void streamData() {
    for (int i = 0; i < this.datapoints.length; i++) {
      this.context.writeEnd(Constants.SimpleGraphConfig.REDUCE_EDGE, this.datapoints[i]);
    }
  }
}
