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

package edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint;

import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.CheckpointingBatchTSetIWorker;

public class TSetCheckptExample implements CheckpointingBatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(TSetCheckptExample.class.getName());

  public static void main(String[] args) {

    JobConfig jobConfig = new JobConfig();

    Twister2Job job = Twister2Job.newBuilder()
        .setJobName(TSetCheckptExample.class.getName())
        .setConfig(jobConfig)
        .setWorkerClass(TSetCheckptExample.class)
        .addComputeResource(1, 512, 4)
        .build();

    Twister2Submitter.submitJob(job);
  }

  @Override
  public void execute(CheckpointingTSetEnv env) {
    LOG.info(String.format("Hello from worker %d", env.getWorkerID()));

    SourceTSet<Integer> sourceX = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 10000;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 4);

    long t1 = System.currentTimeMillis();
    ComputeTSet<Object, Iterator<Object>> twoComputes = sourceX.direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect(i * 5);
      });
    }).direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect((int) i + 2);
      });
    });
    LOG.info("Time for two computes : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    PersistedTSet<Object> persist = twoComputes.persist();
    LOG.info("Time for persist : " + (System.currentTimeMillis() - t1) / 1000);
    // When persist() is called, twister2 performs all the computations/communication
    // upto this point and persists the result into the disk.
    // This makes previous data garbage collectible and frees some memory.
    // If persist() is called in a checkpointing enabled job, this will create
    // a snapshot at this point and will start straightaway from this point if the
    // job is restarted.

    // Similar to CachedTSets, PersistedTSets can be added as inputs for other TSets and
    // operations


    persist.reduce((i1, i2) -> {
      return (int) i1 + (int) i2;
    }).forEach(i -> {
      LOG.info("SUM=" + i);
    });
  }
}
