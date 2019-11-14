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
package edu.iu.dsc.tws.examples.tset.checkpointing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.CheckpointingBatchTSetIWorker;

public class TSetFTExample implements CheckpointingBatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(TSetFTExample.class.getName());

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(TSetFTExample.class.getName())
        .setWorkerClass(TSetFTExample.class)
        .addComputeResource(1, 512, 2)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  @Override
  public void execute(CheckpointingTSetEnv env) {
    LOG.info("Starting worker...");

    // testing variable loading
    long timeNow = System.currentTimeMillis();
    long initTime = env.initVariable("test-time-var", timeNow);

    if (initTime == timeNow) {
      LOG.info("Variable [not] loaded from snapshot");
    } else {
      LOG.info("Variable [loaded] from snapshot");
    }
    env.commit();

    SourceTSet<Integer> source = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 10;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 2);

    PersistedTSet<Integer> cache = source.direct().persist();

    cache.direct().forEach(i -> {
      LOG.info("i : " + i);
    });

  }
}
