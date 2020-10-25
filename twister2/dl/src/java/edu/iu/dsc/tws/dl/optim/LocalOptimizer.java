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
import edu.iu.dsc.tws.dl.criterion.AbstractCriterion;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.TensorPair;

public class LocalOptimizer<T> extends Optimizer<T> {


  public LocalOptimizer(AbstractModule model, BatchTSet<T> dataset, AbstractCriterion criterion) {
    super(model, dataset, criterion);
  }

  @Override
  public AbstractModule optimize() {
    AbstractModule modal = this.getModel();
    AbstractCriterion criterion = this.getCriterion();
    TensorPair parameters = this.getModel().getParameters();
    //TODO check of the exsiting array can be used
    DoubleDoubleArrayPair result = new DoubleDoubleArrayPair(0.0,
        new double[modal.getParameters().getValue1().storage().length()]);

    this.getDataset().direct().map(new TrainMapFunction<T>(modal, criterion))
        .allReduce(new TrainReduceFunction(result)).map(new AverageParameters()).direct()
        .forEach(data -> System.out.println("Loss value : " + data.getValue0() + " \n"
            + "Grad : " + data.getValue1()[0]));
    return null;
  }

}
