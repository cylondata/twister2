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
import edu.iu.dsc.tws.dl.data.dataset.DataSetFactory;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.AbstractModule;
import edu.iu.dsc.tws.dl.module.mkldnn.MklDnnModule;
import edu.iu.dsc.tws.dl.utils.ConversionUtils;
import edu.iu.dsc.tws.dl.utils.graph.IRGraph;
import edu.iu.dsc.tws.dl.utils.pair.DoubleDoubleArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.DoubleTensorPair;
import edu.iu.dsc.tws.dl.utils.pair.FloatFloatArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.PrimitiveArrayPair;
import edu.iu.dsc.tws.dl.utils.pair.TensorPair;
import edu.iu.dsc.tws.dl.utils.schema.DLSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;

public class DistributedOptimizerCustomPacker<A extends Tensor, T> extends Optimizer<T> {


  public DistributedOptimizerCustomPacker(BatchEnvironment env, AbstractModule model,
                                          BatchTSet<T> dataset, AbstractCriterion criterion) {
    super(env, model, dataset, criterion);
  }

  @Override
  public AbstractModule optimize() {
    long startTime = System.nanoTime();
    long dataLoatTime = System.nanoTime();

    double[] loss = new double[1];
    AbstractModule<A> modal = this.getModel();

    if (this.isMklDnn() && !(modal instanceof MklDnnModule)
        && !(modal instanceof IRGraph)) {
      modal = modal.toGraph().setName(modal.getName());
      modal.toFloat();
      modal.setMklDnn(true);
    }

    modal = ConversionUtils.convert(modal);

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
    PrimitiveArrayPair result;
    if (modal.isFloat()) {
      result = new FloatFloatArrayPair(0.0f,
          new float[grad.storage().length()]);
    } else {
      result = new DoubleDoubleArrayPair(0.0,
          new double[grad.storage().length()]);
    }

    dataLoatTime = System.nanoTime() - dataLoatTime;
    long iterationTime = System.nanoTime();

    //TODO use caching TSet
    CachedTSet<PrimitiveArrayPair> trainResult;
    T currentData = cachedData.get(currentIteration);
    CachedTSet<AbstractModule> modalTSet = DataSetFactory
        .createModalDataSet(env, modal, parallelism)
        .cache();

    CachedTSet<T> src = DataSetFactory.createSingleDataSet(env, currentData, parallelism).cache();
    DataObject<T> iterationData;
    DataObject<AbstractModule> iterationModal;

    ComputeTSet<PrimitiveArrayPair> trainMap = src.direct()
        .map(new TrainMapFunction<A, T>(criterion));

    //input the model
    trainMap.addInput("modal", modalTSet);
    if (modal.isFloat()) {
      trainResult = trainMap
          .allReduce(new TrainReduceFunction(result, env.getWorkerID(),
              modal.isFloat())).map(new AverageParameters()).lazyCache();
    } else {
      trainResult = trainMap.withSchema(new DLSchema())
          .allReduce(new TrainReduceFunction(result, env.getWorkerID(),
              modal.isFloat())).map(new AverageParameters())
          .withSchema(new DLSchema()).lazyCache();
    }

    while (!this.getEndWhen().apply(this.state)) {
      env.eval(trainResult);
      List<PrimitiveArrayPair> resultValues = trainResult.getData();
      DoubleTensorPair resultPair;
      if (modal.isFloat()) {
        resultPair = new DoubleTensorPair(((FloatFloatArrayPair) resultValues.get(0)).getValue0(),
            new DenseTensor(((FloatFloatArrayPair) resultValues.get(0)).getValue1()));
      } else {
        resultPair = new DoubleTensorPair(((DoubleDoubleArrayPair) resultValues.get(0)).getValue0(),
            new DenseTensor(((DoubleDoubleArrayPair) resultValues.get(0)).getValue1()));
      }


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
      iterationData = DataSetFactory.createDataObject(env, currentData);
      iterationModal = DataSetFactory.createDataObject(env, modal);
//      iterationData = DataSet.createSingleDataSet(env, currentData, parallelism).cache();
//      iterationModal = DataSet.createModalDataSet(env, modal, parallelism).cache();
      env.updateTSet(iterationData, src);
      env.updateTSet(iterationModal, modalTSet);
    }
    long endTime = System.nanoTime();
    System.out.println("Rank" + env.getWorkerID() + " Data Time : " + dataLoatTime / 1e6 + "ms");
    if (env.getWorkerID() == 0) {
      System.out.println("Total Optimizer Time : " + (endTime - startTime) / 1e6 + "ms");
      System.out.println("Data Load Time : " + dataLoatTime / 1e6 + "ms");
      System.out.println("Iteration Time : " + (endTime - iterationTime) / 1e6 + "ms");
    }

    env.finishEval(trainResult);
    return this.getModel();
  }

}
