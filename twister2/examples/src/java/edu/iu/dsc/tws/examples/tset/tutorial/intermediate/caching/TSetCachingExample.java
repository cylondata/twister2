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

package edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching;

import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;

public class TSetCachingExample implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(TSetCachingExample.class.getName());

  public static void main(String[] args) {

    JobConfig jobConfig = new JobConfig();

    Twister2Job job = Twister2Job.newBuilder()
        .setJobName(TSetCachingExample.class.getName())
        .setConfig(jobConfig)
        .setWorkerClass(TSetCachingExample.class)
        .addComputeResource(1, 512, 4)
        .build();

    Twister2Submitter.submitJob(job);
  }

  @Override
  public void execute(BatchTSetEnvironment env) {
    LOG.info(String.format("Hello from worker %d", env.getWorkerID()));

    SourceTSet<Integer> sourceX = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 10;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 4);

    ComputeTSet<Object, Iterator<Object>> twoComputes = sourceX.direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect(i * 5);
      });
    }).direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect((int) i + 2);
      });
    });

    CachedTSet<Object> cached = twoComputes.cache();
    // when cache is called, twister2 will run everything upto this point and cache the result
    // into the memory. Cached TSets can be added as inputs for other TSets and operations.

    SourceTSet<Integer> sourceZ = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 10;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 4);

    ComputeTSet<Integer, Iterator<Integer>> calc = sourceZ.direct().compute(
        new ComputeCollectorFunc<Integer, Iterator<Integer>>() {

          private DataPartitionConsumer<Integer> xValues;

          @Override
          public void prepare(TSetContext context) {
            this.xValues = (DataPartitionConsumer<Integer>) context.getInput("x").getConsumer();
          }

          @Override
          public void compute(Iterator<Integer> zValues, RecordCollector<Integer> output) {
            while (zValues.hasNext()) {
              output.collect(xValues.next() + zValues.next());
            }
          }
        });

    calc.addInput("x", cached);

    calc.direct().forEach(i -> {
      LOG.info("(x * 5) + 2 + z =" + i);
    });
  }
}
