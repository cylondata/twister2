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

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.dl.criterion.AbstractCriterion;
import edu.iu.dsc.tws.dl.data.Tensor;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.DoubleTensorPair;
import edu.iu.dsc.tws.dl.utils.pair.TensorPair;

public class LocalOptimizer<T> extends Optimizer<T> {


  public LocalOptimizer(AbstractModule model, BatchTSet<T> dataset, AbstractCriterion criterion) {
    super(model, dataset, criterion);
  }

  @Override
  public AbstractModule optimize() {
    double[] loss = new double[1];
    AbstractModule modal = this.getModel();
    AbstractCriterion criterion = this.getCriterion();
    TensorPair parameters = this.getModel().getParameters();
    Tensor weight = parameters.getValue0();
    Tensor grad = parameters.getValue1();
    Map<String, OptimMethod> optimMethods = this.getOptimMethods();
    this.state.put("epoch", this.state.getOrDefault("epoch", 1));
    this.state.put("neval", this.state.getOrDefault("neval", 1));

    //TODO check of the exsiting array can be used
    DoubleDoubleArrayPair result = new DoubleDoubleArrayPair(0.0,
        new double[grad.storage().length()]);

    //TODO use caching TSet
    StorableTBase<DoubleDoubleArrayPair> trainResult;
    while (!this.getEndWhen().apply(this.state)) {
      trainResult = this.getDataset().direct()
          .map(new TrainMapFunction<T>(modal, criterion))
          .allReduce(new TrainReduceFunction(result)).map(new AverageParameters()).cache();

      List<DoubleDoubleArrayPair> resultValues = trainResult.getData();
      DoubleTensorPair resultPair = new DoubleTensorPair(resultValues.get(0).getValue0(),
          new DenseTensor(resultValues.get(0).getValue1()));

      //TODO need to support individual layer optimizer methods later. this would mean updating the
      // weight values need to be updated using tensors.
      for (Map.Entry<String, OptimMethod> optimMethodEntry : optimMethods.entrySet()) {
        optimMethodEntry.getValue().optimize(t -> resultPair, weight);
      }
      System.out.println("Loss : " + resultPair.getValue0());
      this.state.put("epoch", this.state.getOrDefault("epoch", 1) + 1);
      this.state.put("neval", this.state.getOrDefault("neval", 1) + 1);
    }


    return null;
  }

}
