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
import edu.iu.dsc.tws.dl.criterion.AbstractCriterion;
import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.minibatch.ArrayTensorMiniBatch;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;

public class TrainMapFunction<T> extends BaseMapFunc<T, double[]> {

  private AbstractModule modal;
  private AbstractCriterion criterion;

  public TrainMapFunction(AbstractModule localModal, AbstractCriterion localCriterion) {
    this.modal = localModal;
    this.criterion = localCriterion;
  }

  @Override
  public double[] map(T data) {
    double[] result = new double[1];
    ArrayTensorMiniBatch miniBatch = (ArrayTensorMiniBatch) data;
    modal.zeroGradParameters();
    modal.training();
    Activity input = miniBatch.getInput();
    Activity target = miniBatch.getTarget();
    DenseTensor output = modal.forward((DenseTensor) input);
    double loss = criterion.forward(output, target);
    Activity errors = criterion.backward(output, target);
    modal.backward((DenseTensor) input, (DenseTensor) errors);
    System.out.println("Loss Rank : " + this.getTSetContext().getIndex() + " : " + loss);
    result[0] = loss;
    return result;
  }
}
