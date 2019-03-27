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
import edu.iu.dsc.tws.examples.ml.svm.test.Predict;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.DataUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;

public class SvmTestMap extends BaseMapFunction<double[][], Double> {

  private static final Logger LOG = Logger.getLogger(SvmTestMap.class.getName());

  private BinaryBatchModel binaryBatchModel;

  private SVMJobParameters svmJobParameters;

  private Predict predict;

  private double localAccuracy = 0;

  public SvmTestMap(BinaryBatchModel binaryBatchModel, SVMJobParameters svmJobParameters) {
    this.binaryBatchModel = binaryBatchModel;
    this.svmJobParameters = svmJobParameters;
  }

  @Override
  public Double map(double[][] doubles) {
    this.binaryBatchModel = DataUtils.updateModelData(this.binaryBatchModel, doubles);
    this.predict = new Predict(this.binaryBatchModel.getX(), this.binaryBatchModel.getY(),
        this.binaryBatchModel.getW());
    try {
      this.localAccuracy = this.predict.predict();
    } catch (MatrixMultiplicationException e) {
      e.printStackTrace();
    }
    LOG.info(String.format("Local Acc [%d] %f", this.context.getIndex(), this.localAccuracy));
    return this.localAccuracy;
  }

  @Override
  public void prepare() {

  }
}
