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
package edu.iu.dsc.tws.dl.optim;

import edu.iu.dsc.tws.api.tset.fn.BaseMapFunc;
import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.minibatch.ArrayTensorMiniBatch;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;

public class PredictClassMapFunction<T> extends BaseMapFunc<T, int[]> {

  private AbstractModule modal;

  public PredictClassMapFunction(AbstractModule predictionModal) {
    this.modal = predictionModal;
  }

  @Override
  public int[] map(T input) {

    ArrayTensorMiniBatch miniBatch = (ArrayTensorMiniBatch) input;
    modal.evaluate();
    Activity data = miniBatch.getInput();
    DenseTensor output = modal.forward((DenseTensor) input);
    return new int[0];
  }
}
