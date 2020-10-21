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

import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.dl.criterion.Criterion;
import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.data.minibatch.ArrayTensorMiniBatch;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;

public class LocalOptimizer<T> extends Optimizer<T> {


  public LocalOptimizer(AbstractModule model, BatchTSet<T> dataset, Criterion criterion) {
    super(model, dataset, criterion);
  }

  @Override
  public AbstractModule optimize() {
    AbstractModule modal = this.getModel();
    Criterion criterion = this.getCriterion();

    this.getDataset().direct().forEach(data -> {
      ArrayTensorMiniBatch miniBatch = (ArrayTensorMiniBatch) data;
      modal.zeroGradParameters();
      modal.training();
      Activity input = miniBatch.getInput();
      DenseTensor output = modal.forward((DenseTensor) input);
      System.out.println(output.toString());
    });
    return null;
  }
}
