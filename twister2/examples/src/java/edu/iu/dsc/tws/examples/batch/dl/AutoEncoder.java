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
import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.dl.criterion.AbstractCriterion;
import edu.iu.dsc.tws.dl.criterion.MSECriterion;
import edu.iu.dsc.tws.dl.data.MiniBatch;
import edu.iu.dsc.tws.dl.data.dataset.DataSet;
import edu.iu.dsc.tws.dl.graph.Sequential;
import edu.iu.dsc.tws.dl.module.Linear;
import edu.iu.dsc.tws.dl.module.ReLU;
import edu.iu.dsc.tws.dl.module.Reshape;
import edu.iu.dsc.tws.dl.module.Sigmoid;
import edu.iu.dsc.tws.dl.optim.Adam;
import edu.iu.dsc.tws.dl.optim.LocalOptimizer;
import edu.iu.dsc.tws.dl.optim.Optimizer;
import edu.iu.dsc.tws.dl.optim.trigger.Triggers;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
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
    Config config = env.getConfig();
    int parallelism = config.getIntegerValue("parallelism");
    int dataSize = config.getIntegerValue("dataSize");
    int batchSize = config.getIntegerValue("batchSize");

    String dataFile = "/home/pulasthi/work/thesis/data/csv/20.csv";
    SourceTSet<MiniBatch> source = DataSet
        .createMiniBatchDataSet(env, dataFile, batchSize, dataSize, parallelism);

    //Define model
    Sequential model = new Sequential();
    model.add(new Reshape(new int[]{12}));
    model.add(new Linear(12, 3));
    model.add(new ReLU(false));
    model.add(new Linear(3, 12));
    model.add(new Sigmoid());
    //criterion
    AbstractCriterion criterion = new MSECriterion();

    //Define Oprimizer
    Optimizer<MiniBatch> optimizer = new LocalOptimizer<MiniBatch>(env, model, source, criterion);
    optimizer.setOptimMethod(new Adam());
    optimizer.setEndWhen(Triggers.maxEpoch(10));
    optimizer.optimize();
  }

  public static void main(String[] args) {
    // lets take number of workers as an command line argument
    int numberOfWorkers = 1;
    int batchSize = 0;
    int dataSize = 0;
    if (args.length >= 3) {
      numberOfWorkers = Integer.valueOf(args[0]);
      batchSize = Integer.valueOf(args[1]);
      dataSize = Integer.valueOf(args[2]);
    } else {
      throw new IllegalStateException("need to provide parallelism, batchSize and dataSize");
    }

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("dnn-key", "Twister2-DNN");
    jobConfig.put("parallelism", numberOfWorkers);
    jobConfig.put("batchSize", batchSize);
    jobConfig.put("dataSize", dataSize);

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName("AutoEncoder-job")
        .setWorkerClass(AutoEncoder.class)
        .addComputeResource(.2, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
