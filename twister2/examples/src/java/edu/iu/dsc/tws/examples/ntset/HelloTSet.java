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
package edu.iu.dsc.tws.examples.ntset;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunction;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.fn.Source;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;


public class HelloTSet implements IWorker, Serializable {
  private static final int COUNT = 10;
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    WorkerEnvironment workerEnv = WorkerEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);

    TSetEnvironment tSetEnv = TSetEnvironment.init(workerEnv);

    BatchSourceTSet<Integer> src = tSetEnv.createBatchSource(new Source<Integer>() {
      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < COUNT;
      }

      @Override
      public Integer next() {
        return ++count;
      }
    }, 4).setName("Source");

    ComputeTSet<Integer, Iterator<Integer>> sum = src.direct().compute(
        (ComputeFunction<Integer, Iterator<Integer>>) input -> {
          int s = 0;
          while (input.hasNext()) {
            s += input.next();
          }
          return s;
        }).setName("sum");


    SinkTSet<Integer> sink = sum.direct().sink((Sink<Integer>) value -> {
      System.out.println("val: " + value);
      return true;
    });

//    sink.collect();

//    src.reduce((IFunction<Integer>) Integer::sum).map((MapFunction<Integer, Integer>)
//    i -> i + 100).collect();

//    System.out.println(Arrays.toString(out.toArray()));
  }


  public static void main(String[] args) {
/*
    // first load the configurations from command line and config files
    Options options = new Options();
    options.addOption("para", true, "Workers");
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine cmd = commandLineParser.parse(options, args);

    int para = Integer.parseInt(cmd.getOptionValue("para"));
    // build JobConfig
*/
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    submitJob(config, 4, jobConfig, HelloTSet.class.getName());
  }

  private static void submitJob(Config config, int containers, JobConfig jobConfig, String clazz) {
    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(clazz)
        .setWorkerClass(clazz)
        .addComputeResource(1, 512, containers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}