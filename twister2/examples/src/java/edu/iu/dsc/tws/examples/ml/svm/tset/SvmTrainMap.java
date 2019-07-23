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
package edu.iu.dsc.tws.examples.ml.svm.tset;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseTFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos.PegasosSgdSvm;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;

public class SvmTrainMap extends BaseTFunction implements MapFunc<double[], double[][]> {

  private static final Logger LOG = Logger.getLogger(SvmTrainMap.class.getName());

  private double[] w;

  private BinaryBatchModel binaryBatchModel;

  private SVMJobParameters svmJobParameters;

  private PegasosSgdSvm pegasosSgdSvm;

  private boolean debug = false;


  public SvmTrainMap(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
    this.w = this.binaryBatchModel.getW();
  }

  @Override
  public double[] map(double[][] dataPoints) {
    if (debug) {
      LOG.info(String.format("Training Dimensions [%d,%d]", dataPoints.length, dataPoints[0]
          .length));
    }
    this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel, dataPoints);
    this.binaryBatchModel.setW(
        (double[]) getTSetContext().getInput(Constants.SimpleGraphConfig.INPUT_WEIGHT_VECTOR)
            .getPartition(0).getConsumer().next());
    // todo: this is not the best way to do it! partitionID should correspond to task ID

    this.pegasosSgdSvm = new PegasosSgdSvm(this.binaryBatchModel.getW(),
        this.binaryBatchModel.getX(), this.binaryBatchModel.getY(),
        this.binaryBatchModel.getAlpha(), this.binaryBatchModel.getIterations(),
        this.binaryBatchModel.getFeatures());
    try {
      this.pegasosSgdSvm.iterativeTaskSgd(this.binaryBatchModel.getW(),
          this.binaryBatchModel.getX(),
          this.binaryBatchModel.getY());
    } catch (NullDataSetException e) {
      e.printStackTrace();
    } catch (MatrixMultiplicationException e) {
      e.printStackTrace();
    }
    return this.pegasosSgdSvm.getW();
  }
}
