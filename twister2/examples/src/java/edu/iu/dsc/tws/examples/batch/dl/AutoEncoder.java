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
package edu.iu.dsc.tws.examples.batch.dl;


import java.io.Serializable;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.dl.criterion.Criterion;
import edu.iu.dsc.tws.dl.criterion.MSECriterion;
import edu.iu.dsc.tws.dl.data.Sample;
import edu.iu.dsc.tws.dl.graph.Sequential;
import edu.iu.dsc.tws.dl.module.Linear;
import edu.iu.dsc.tws.dl.module.ReLU;
import edu.iu.dsc.tws.dl.module.Reshape;
import edu.iu.dsc.tws.dl.module.Sigmoid;
import edu.iu.dsc.tws.dl.optim.LocalOptimizer;
import edu.iu.dsc.tws.dl.optim.Optimizer;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

/**
 * Simple AutoEncoder example
 */
public class AutoEncoder implements Twister2Worker, Serializable {
  private static final Logger LOG = Logger.getLogger(AutoEncoder.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);

    SourceTSet<Sample> source = null;

    //Define model
    Sequential model = new Sequential();
    model.add(new Reshape(new int[]{784}));
    model.add(new Linear(784, 32));
    model.add(new ReLU(false));
    model.add(new Linear(32, 784));
    model.add(new Sigmoid());
    //criterion
    Criterion criterion = new MSECriterion();

    //Define Oprimizer
    Optimizer<Sample> optimizer = new LocalOptimizer<>(model, source, criterion);

    optimizer.optimize();

  }
}
