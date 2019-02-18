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
package edu.iu.dsc.tws.examples.tset;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetBuilder;
import edu.iu.dsc.tws.api.tset.fn.LoadBalancePartitioner;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;

public class HelloTSet extends TaskWorker implements Serializable {
  private static final long serialVersionUID = -2;
  @Override
  public void execute() {
    TSetBuilder builder = TSetBuilder.newBuilder(config);
    TSet<int[]> source = builder.createSource(new Source<int[]>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 1;
      }

      @Override
      public int[] next() {
        count++;
        return new int[]{1, 1, 1};
      }
    }).setName("Source");

    TSet<int[]> partitioned = source.partition(new LoadBalancePartitioner<>());

    TSet<int[]> reduce = partitioned.reduce((t1, t2) -> {
      int[] ret = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        ret[i] = t1[i] + t2[i];
      }
      return ret;
    }).setName("Reduce");

    reduce.sink(value -> {
      System.out.println(Arrays.toString(value));
      return false;
    });
    DataFlowTaskGraph graph = builder.build();

    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, executionPlan);
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    // build JobConfig
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
