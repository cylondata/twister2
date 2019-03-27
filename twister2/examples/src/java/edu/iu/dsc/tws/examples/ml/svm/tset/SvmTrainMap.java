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

import edu.iu.dsc.tws.api.tset.BaseMapFunction;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos.PegasosSgdSvm;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;

public class SvmTrainMap extends BaseMapFunction<double[][], double[]> {

  private static final Logger LOG = Logger.getLogger(SvmTrainMap.class.getName());

  private double[] w;

  private BinaryBatchModel binaryBatchModel;

  private SVMJobParameters svmJobParameters;

  private PegasosSgdSvm pegasosSgdSvm;

  public SvmTrainMap(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
  }

  @Override
  public double[] map(double[][] dataPoints) {
    this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel, dataPoints);
    this.pegasosSgdSvm = new PegasosSgdSvm(this.binaryBatchModel.getW(),
        this.binaryBatchModel.getX(), this.binaryBatchModel.getY(),
        this.binaryBatchModel.getAlpha(), this.binaryBatchModel.getIterations(),
        this.binaryBatchModel.getFeatures());
    try {
      this.pegasosSgdSvm.iterativeSgd(this.binaryBatchModel.getW(), this.binaryBatchModel.getX(),
          this.binaryBatchModel.getY());
    } catch (NullDataSetException e) {
      e.printStackTrace();
    } catch (MatrixMultiplicationException e) {
      e.printStackTrace();
    }
    return this.pegasosSgdSvm.getW();
  }

  @Override
  public void prepare() {
    this.w = this.binaryBatchModel.getW();
  }
}
