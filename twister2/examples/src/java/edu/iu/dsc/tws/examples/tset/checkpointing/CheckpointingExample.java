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
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchChkPntEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class CheckpointingExample implements Twister2Worker, Serializable {
  private static final Logger LOG = Logger.getLogger(CheckpointingExample.class.getName());
  private static final int PAR = 2;

  private SourceTSet<Integer> dummySource(BatchChkPntEnvironment env, int count,
                                          int init) {
    return env.createSource(new SourceFunc<Integer>() {
      private int c = init;

      @Override
      public boolean hasNext() {
        return c < count + init;
      }

      @Override
      public Integer next() {
        return c++;
      }
    }, PAR);
  }

  @Override
  public void execute(WorkerEnvironment workerEnvironment) {

    BatchChkPntEnvironment env = TSetEnvironment.initCheckpointing(workerEnvironment);
    int count = 5;

    SourceTSet<Integer> src = dummySource(env, count, 100 * env.getWorkerID());
    PersistedTSet<Integer> persist = src.direct().persist();

    SourceTSet<Integer> src1 = dummySource(env, count, 100 * env.getWorkerID() + 10);
    src1.direct().compute(
        new BaseComputeFunc<Iterator<Integer>, String>() {
          private DataPartitionConsumer<Integer> in;

          @Override
          public void prepare(TSetContext ctx) {
            super.prepare(ctx);
            in = (DataPartitionConsumer<Integer>) ctx.getInput("in").getConsumer();
          }

          @Override
          public String compute(Iterator<Integer> input) {
            StringBuilder out = new StringBuilder();
            while (input.hasNext() && in.hasNext()) {
              out.append("(").append(input.next()).append(",").append(in.next()).append(") ");
            }
            return out.toString();
          }
        }
    ).addInput("in", persist).direct().forEach(i -> LOG.info(i));

    persist.direct().forEach(i -> LOG.info(i.toString()));

  }

  public static void main(String[] args) {
    HashMap<String, Object> map = new HashMap<>();
//    map.put("twister2.checkpointing.enable", true);
    Config config = ResourceAllocator.loadConfig(map);

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PAR, jobConfig, CheckpointingExample.class.getName());
  }
}
