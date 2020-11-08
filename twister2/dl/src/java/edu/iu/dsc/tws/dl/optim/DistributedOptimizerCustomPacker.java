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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.dl.criterion.AbstractCriterion;
import edu.iu.dsc.tws.dl.data.Tensor;
import edu.iu.dsc.tws.dl.data.dataset.DataSet;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.DoubleTensorPair;
import edu.iu.dsc.tws.dl.utils.pair.TensorPair;
import edu.iu.dsc.tws.dl.utils.schema.DLSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;

public class DistributedOptimizerCustomPacker<T> extends Optimizer<T> {


  public DistributedOptimizerCustomPacker(BatchEnvironment env, AbstractModule model,
                                          BatchTSet<T> dataset, AbstractCriterion criterion) {
    super(env, model, dataset, criterion);
  }

  @Override
  public AbstractModule optimize() {
    long startTime = System.nanoTime();
    long dataLoatTime = System.nanoTime();

    double[] loss = new double[1];
    AbstractModule modal = this.getModel();
    AbstractCriterion criterion = this.getCriterion();
    TensorPair parameters = this.getModel().getParameters();
    Tensor weight = parameters.getValue0();
    Tensor grad = parameters.getValue1();
    Map<String, OptimMethod> optimMethods = this.getOptimMethods();
    this.state.put("epoch", this.state.getOrDefault("epoch", 1));
    this.state.put("neval", this.state.getOrDefault("neval", 1));
    int epoch = 1;
    int iterationsPerEpoch = 1;
    int currentIteration = 0;
    Config config = env.getConfig();
    int parallelism = config.getIntegerValue("parallelism");
    //Load data
    StorableTBase<T> cachedDataTSet = this.getDataset().cache();
    List<T> cachedData = cachedDataTSet.getData();
    iterationsPerEpoch = cachedData.size();
    //TODO check of the exsiting array can be used
    DoubleDoubleArrayPair result = new DoubleDoubleArrayPair(0.0,
        new double[grad.storage().length()]);
    dataLoatTime = System.nanoTime() - dataLoatTime;
    long iterationTime = System.nanoTime();

    //TODO use caching TSet
    CachedTSet<DoubleDoubleArrayPair> trainResult;
    T currentData = cachedData.get(currentIteration);
    CachedTSet<AbstractModule> modalTSet = DataSet.createModalDataSet(env, modal, parallelism)
        .cache();

    CachedTSet<T> src = DataSet.createSingleDataSet(env, currentData, parallelism).cache();
    DataObject<T> iterationData;
    DataObject<AbstractModule> iterationModal;

    ComputeTSet<DoubleDoubleArrayPair> trainMap = src.direct()
        .map(new TrainMapFunction<T>(criterion));

    //input the model
    trainMap.addInput("modal", modalTSet);
    trainResult = trainMap.withSchema(new DLSchema())
        .allReduce(new TrainReduceFunction(result, env.getWorkerID())).map(new AverageParameters())
        .withSchema(new DLSchema()).lazyCache();

    while (!this.getEndWhen().apply(this.state)) {
      env.eval(trainResult);
      List<DoubleDoubleArrayPair> resultValues = trainResult.getData();
      DoubleTensorPair resultPair = new DoubleTensorPair(resultValues.get(0).getValue0(),
          new DenseTensor(resultValues.get(0).getValue1()));

      //TODO need to support individual layer optimizer methods later. this would mean updating the
      // weight values need to be updated using tensors.
      for (Map.Entry<String, OptimMethod> optimMethodEntry : optimMethods.entrySet()) {
        optimMethodEntry.getValue().optimize(t -> resultPair, weight);
      }

      if (env.getWorkerID() == 0) {
//        System.out.printf("Optimizer "
//                + "[Epoch %d]"
//                + "[Iteration %d] "
//                + "Loss : %f \n",
//            epoch, state.<Integer>get("neval"), resultPair.getValue0());
      }
      currentIteration++;
      if (currentIteration == iterationsPerEpoch) {
        this.state.put("epoch", this.state.getOrDefault("epoch", 1) + 1);
        currentIteration = 0;
        epoch++;
      }
      this.state.put("neval", this.state.getOrDefault("neval", 1) + 1);
      currentData = cachedData.get(currentIteration);
      iterationData = DataSet.createDataObject(env, currentData);
      iterationModal = DataSet.createDataObject(env, modal);
//      iterationData = DataSet.createSingleDataSet(env, currentData, parallelism).cache();
//      iterationModal = DataSet.createModalDataSet(env, modal, parallelism).cache();
      env.updateTSet(iterationData, src);
      env.updateTSet(iterationModal, modalTSet);
    }
    long endTime = System.nanoTime();
    if (env.getWorkerID() == 0) {
      System.out.println("Total Optimizer Time : " + (endTime - startTime) / 1e6 + "ms");
      System.out.println("Data Load Time : " + dataLoatTime / 1e6 + "ms");
      System.out.println("Iteration Time : " + (endTime - iterationTime) / 1e6 + "ms");
    }

    env.finishEval(trainResult);
    return this.getModel();
  }

}
