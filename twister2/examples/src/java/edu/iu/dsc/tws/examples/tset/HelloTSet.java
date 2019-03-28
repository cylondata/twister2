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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetBatchWorker;
import edu.iu.dsc.tws.api.tset.TwisterBatchContext;
import edu.iu.dsc.tws.api.tset.fn.LoadBalancePartitioner;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableMapTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class HelloTSet extends TSetBatchWorker implements Serializable {
  private static final Logger LOG = Logger.getLogger(HelloTSet.class.getName());

  private static final long serialVersionUID = -2;

  @Override
  public void execute(TwisterBatchContext tc) {
    LOG.info("Strating Hello TSet Example");
    BatchSourceTSet<int[]> source = tc.createSource(new Source<int[]>() {

      @Override
      public void prepare() {

      }

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 4;
      }

      @Override
      public int[] next() {
        count++;
        return new int[]{1, 1, 1};
      }
    }, 4).setName("Source");

    PartitionTLink<int[]> partitioned = source.
        partition(new LoadBalancePartitioner<>());
    IterableMapTSet<int[], int[]> mapedPartition =
        partitioned.map((IterableMapFunction<int[], int[]>) ints -> {
          LOG.info("tests");
          if (ints.iterator().hasNext()) {
            return ints.iterator().next();
          } else {
            return new int[0];
          }
        },
            4);

    ReduceTLink<int[]> reduce = mapedPartition.reduce((t1, t2) -> {
      int[] ret = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        ret[i] = t1[i] + t2[i];
      }
      return ret;
    });

    reduce.sink(value -> {
      LOG.info("Results " + Arrays.toString(value));
      return false;
    });

    LOG.info("Ending  Hello TSet Example");

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
