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
import edu.iu.dsc.tws.api.comms.structs.Tuple;
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
import edu.iu.dsc.tws.tset.sets.batch.KeyedPersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;

public class KeyedCheckpointingExample implements Twister2Worker, Serializable {
  private static final Logger LOG = Logger.getLogger(KeyedCheckpointingExample.class.getName());
  private static final int PAR = 2;

  private KeyedSourceTSet<String, Integer> dummySource(BatchChkPntEnvironment env, int count,
                                                       int init) {
    return env.createKeyedSource(new SourceFunc<Tuple<String, Integer>>() {
      private int c = init;

      @Override
      public boolean hasNext() {
        return c < count + init;
      }

      @Override
      public Tuple<String, Integer> next() {
        c++;
        return new Tuple<>(Integer.toString(c), c);
      }
    }, PAR);
  }

  @Override
  public void execute(WorkerEnvironment workerEnvironment) {

    BatchChkPntEnvironment env = TSetEnvironment.initCheckpointing(workerEnvironment);
    int count = 5;

    KeyedSourceTSet<String, Integer> src = dummySource(env, count, 0);
    KeyedPersistedTSet<String, Integer> persist = src.keyedDirect().persist();

    persist.keyedDirect().forEach(i -> LOG.info(i.toString()));

    KeyedSourceTSet<String, Integer> src1 = dummySource(env, count, 10);
    src1.keyedDirect().compute(
        new BaseComputeFunc<Iterator<Tuple<String, Integer>>, String>() {
          private DataPartitionConsumer<Tuple<String, Integer>> in;

          @Override
          public void prepare(TSetContext ctx) {
            super.prepare(ctx);
            in = (DataPartitionConsumer<Tuple<String, Integer>>) ctx.getInput("in").getConsumer();
          }

          @Override
          public String compute(Iterator<Tuple<String, Integer>> input) {
            StringBuilder out = new StringBuilder();
            while (input.hasNext() && in.hasNext()) {
              Tuple<String, Integer> t = input.next();
              Tuple<String, Integer> next = in.next();
              out.append("(").append(t).append(",").append(next).append(") ");
            }
            return out.toString();
          }
        }
    ).addInput("in", persist).direct().forEach(i -> LOG.info(i));

  }

  public static void main(String[] args) {
    HashMap<String, Object> map = new HashMap<>();
//    map.put("twister2.checkpointing.enable", true);
    Config config = ResourceAllocator.loadConfig(map);

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PAR, jobConfig, KeyedCheckpointingExample.class.getName());
  }
}
