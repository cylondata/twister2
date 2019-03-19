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

import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos.PegasosSgdSvm;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SVMCompute extends BaseCompute {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(SVMCompute.class.getName());

  private boolean debug = false;

  private double[] streamDataPoint;

  private double[][] batchDataPoints;

  private BinaryBatchModel binaryBatchModel;

  private double[] wInit;

  private double[] w;

  private double[] x;

  private double y;

  private OperationMode operationMode;

  private PegasosSgdSvm pegasosSgdSvm;

  private int batchDataCount = 0;

  public SVMCompute(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public SVMCompute(BinaryBatchModel binaryBatchModel, OperationMode operationMode) {
    this.binaryBatchModel = binaryBatchModel;
    this.w = this.binaryBatchModel.getW();
    this.operationMode = operationMode;
    LOG.info(String.format("Initializing SVMCompute : %s", this.binaryBatchModel.toString()));
    initializeStreamMode();
  }

  @Override
  public boolean execute(IMessage content) {

    Object object = content.getContent();

    if (debug) {
      LOG.info("Message Type : " + content.getContent().getClass().getName());
    }

    if (this.operationMode.equals(OperationMode.BATCH)) {
      if (object instanceof Iterator) {
        while (((Iterator) object).hasNext()) {
          this.batchDataCount++;
          Object ret = ((Iterator) object).next();
          if (!(ret instanceof double[][])) {
            LOG.severe(String.format("Something Went Wrong in input data !!!"));
          }
          this.batchDataPoints = (double[][]) ret;
          this.initializeBatchMode();
          this.batchTraining();
          this.context.write(Constants.SimpleGraphConfig.REDUCE_EDGE, this.w);
        }
      }
      LOG.info(String.format("Batch Size : %d, Dimensions of Data [%d,%d] ",
          this.batchDataCount, this.batchDataPoints.length, this.batchDataPoints[0].length));
      this.context.end(Constants.SimpleGraphConfig.REDUCE_EDGE);
    }

    if (this.operationMode.equals(OperationMode.STREAMING)) {
      if (object instanceof double[]) {
        this.streamDataPoint = (double[]) object;
        if (this.streamDataPoint.length == this.binaryBatchModel.getFeatures() + 1) {
          this.y = this.streamDataPoint[0];
          this.x = Arrays.copyOfRange(this.streamDataPoint, 1, this.streamDataPoint.length);
          this.onlineTraining(this.x, this.y);
        } else {
          LOG.severe(String
              .format("Wrong Data Format! DataFormat= {y_i E R^1 (+1 or -1), x_i E R^d"));
        }
        this.context.write(Constants.SimpleGraphConfig.REDUCE_EDGE, this.w);
        // do SVM-SGD computation
      }
    }

    return true;
  }

  /**
   * This method can be used to do the online training
   * Upon the receiving IMessage with a double [] = {y_i, x_i1, ...x_id}
   *
   * @param x1 data point with d elements
   * @param y1 label an integer [-1, +1] for binary classification
   */
  public void onlineTraining(double[] x1, double y1) {

    try {
      pegasosSgdSvm.onlineSGD(this.w, x1, y1);
      this.w = pegasosSgdSvm.getW();
    } catch (NullDataSetException e) {
      LOG.severe(e.getMessage());
    } catch (MatrixMultiplicationException e) {
      LOG.severe(e.getMessage());
    }

  }

  /**
   * This method is used to do batch mode training   *
   */
  public void batchTraining() {
    double[][] x1 = this.binaryBatchModel.getX();
    double[] y1 = this.binaryBatchModel.getY();
    LOG.log(Level.INFO, String.format("Batch Mode Training , Samples %d, Features %d",
        x1.length, x1[0].length));
    try {
      pegasosSgdSvm.iterativeSgd(this.binaryBatchModel.getW(), x1, y1);
      this.w = pegasosSgdSvm.getW();
    } catch (NullDataSetException e) {
      e.printStackTrace();
    } catch (MatrixMultiplicationException e) {
      e.printStackTrace();
    }
  }

  /**
   * This method can be initialized when constructor is initialized   *
   */
  public void initializeStreamMode() {
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      pegasosSgdSvm = new PegasosSgdSvm(this.binaryBatchModel.getW(),
          this.binaryBatchModel.getAlpha(), 1, this.binaryBatchModel.getFeatures());
    }
  }

  /**
   * This method initializes the parameters needed to run the batch mode algorithm
   * This method is called per a received batch
   */
  public void initializeBatchMode() {
    this.initializeBinaryModel(this.batchDataPoints);
    pegasosSgdSvm = new PegasosSgdSvm(this.binaryBatchModel.getW(), this.binaryBatchModel.getX(),
        this.binaryBatchModel.getY(), this.binaryBatchModel.getAlpha(),
        this.binaryBatchModel.getIterations(), this.binaryBatchModel.getFeatures());
  }

  /**
   * This method is deprecated and in the new api the model is initialized before job submission
   *
   * @deprecated must be initialized before main initialization
   */
  @Deprecated
  public void initializeBinaryModel(double[][] xy, int iterations, double alpha) {
    binaryBatchModel = DataUtils.generateBinaryModel(xy, iterations, alpha);
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
    LOG.info("Binary Batch Model Before Updated : " + this.binaryBatchModel.toString());
    this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel, xy);
    LOG.info("Binary Batch Model After Updated : " + this.binaryBatchModel.toString());
    LOG.info(String.format("Updated Data [%d,%d] ",
        this.binaryBatchModel.getX().length, this.binaryBatchModel.getX()[0].length));
  }

  /**
   * Initializes weights with a Gaussian distribution
   */


}
