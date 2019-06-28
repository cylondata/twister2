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
package edu.iu.dsc.tws.examples.ml.svm.util;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.sgd.pegasos.PegasosSgdSvm;
import edu.iu.dsc.tws.examples.ml.svm.test.Predict;

public final class MLUtils {

  private static final Logger LOG = Logger.getLogger(MLUtils.class.getName());

  private static boolean debug = false;

  private MLUtils() {

  }

  public static <T> TrainedModel runIterativeSGDSVM(List<IMessage<T>> data,
                                                    SVMJobParameters svmJobParameters,
                                                    BinaryBatchModel binaryBatchModel,
                                                    String modelName)
      throws NullDataSetException, MatrixMultiplicationException {
    if (debug) {
      LOG.info(String.format("Iterative SGD SVM Training Started"));
    }
    TrainedModel trainedModel = null;
    double[] w = binaryBatchModel.getW();
    PegasosSgdSvm pegasosSgdSvm = new PegasosSgdSvm(binaryBatchModel.getW(),
        svmJobParameters.getAlpha(), svmJobParameters.getIterations(),
        svmJobParameters.getFeatures());
    for (int i = 0; i < svmJobParameters.getIterations(); i++) {
      for (int j = 0; j < data.size(); j++) {
        double[] d = (double[]) data.get(j).getContent();
        double y = d[0];
        double[] x = Arrays.copyOfRange(d, 1, d.length);
        pegasosSgdSvm.onlineSGD(w, x, y);
        w = pegasosSgdSvm.getW();
      }
    }
    binaryBatchModel.setW(w);
    trainedModel = new TrainedModel(binaryBatchModel, 0, modelName);
    if (debug) {
      LOG.info(String.format("Iterative SGD SVM Training End"));
    }
    return trainedModel;
  }

  public static <T> TrainedModel predictSGDSVM(double[] w, double[][] testData,
                                               SVMJobParameters svmJobParameters,
                                               String modelName)
      throws MatrixMultiplicationException {
    TrainedModel trainedModel = null;
    BinaryBatchModel binaryBatchModel = new BinaryBatchModel(svmJobParameters.getSamples(),
        svmJobParameters.getFeatures(), null, w);
    BinaryBatchModel updatedBinaryBatchModel = DataUtils.updateModelData(binaryBatchModel,
        testData);
    Predict predict = new Predict(binaryBatchModel.getX(), binaryBatchModel.getY(), w);
    double acc = predict.predict();
    trainedModel = new TrainedModel(updatedBinaryBatchModel, acc, modelName);
    return trainedModel;
  }
}
