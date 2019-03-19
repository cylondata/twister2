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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.InputDataFormatException;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * This is the DataStreamer for both batch and streaming mode
 * In streaming mode a single data point is sent continously.
 * But in the batch application an array of data points is sent once.
 */
public class DataStreamer extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataStreamer.class.getName());

  private int features = 10;

  private OperationMode operationMode;

  private final double[] labels = {-1, +1};

  private boolean isDummy = false;

  private BinaryBatchModel binaryBatchModel;

  public DataStreamer(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public DataStreamer(int features, OperationMode operationMode) {
    this.features = features;
    this.operationMode = operationMode;
  }

  public DataStreamer(OperationMode operationMode, boolean isDummy,
                      BinaryBatchModel binaryBatchModel) {
    this.operationMode = operationMode;
    this.isDummy = isDummy;
    this.binaryBatchModel = binaryBatchModel;
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

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }

  /**
   * This method is used to deal with dummy data based data stream generation
   * Here data points are generated using a Gaussian Distribution and labels are assigned
   * +1 or -1 randomly for a given data point.
   * @throws InputDataFormatException
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
   * This method must be implemented
   * TODO : Use Twister2 DataAPI to Handle real data streaming
   */
  public void realDataStreamer() {
    // do real data streaming
    throw new org.apache.commons.lang.NotImplementedException("Method Not Implemented");
  }
}
