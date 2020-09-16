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

package edu.iu.dsc.tws.examples.tset.batch;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedCachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;


public class KeyedAddInputsExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(KeyedAddInputsExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    KeyedSourceTSet<String, Integer> src0 = dummyKeyedSource(env, COUNT, PARALLELISM);
    KeyedSourceTSet<String, Integer> src1 = dummyKeyedSourceOther(env, COUNT, PARALLELISM);

    KeyedCachedTSet<String, Integer> cache0 = src0.cache();
    KeyedCachedTSet<String, Integer> cache1 = src1.cache();

    ComputeTSet<String> comp =
        cache0.keyedDirect().compute(
            new BaseComputeCollectorFunc<Iterator<Tuple<String, Integer>>, String>() {
              private Map<String, Integer> input1 = new HashMap<>();

              @Override
              public void prepare(TSetContext ctx) {
                super.prepare(ctx);

                // populate the hashmap with values from the input
                DataPartitionConsumer<Tuple<String, Integer>> part =
                    (DataPartitionConsumer<Tuple<String, Integer>>) getInput("input")
                        .getConsumer();
                while (part.hasNext()) {
                  Tuple<String, Integer> next = part.next();
                  input1.put(next.getKey(), next.getValue());
                }
              }

              @Override
              public void compute(Iterator<Tuple<String, Integer>> input,
                                  RecordCollector<String> output) {
                while (input.hasNext()) {
                  Tuple<String, Integer> next = input.next();
                  output.collect(next.getKey() + " -> " + next.getValue() + ", "
                      + input1.get(next.getKey()));
                }
              }
            }).addInput("input", cache1);

    comp.direct().forEach(i -> LOG.info("comp: " + i));

    LOG.info("Test lazy cache!");

    ComputeTSet<Object> forEach = comp.direct()
        .lazyForEach(i -> LOG.info("comp-lazy: " + i));

    for (int i = 0; i < 4; i++) {
      LOG.info("iter: " + i);
      env.eval(forEach);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    env.finishEval(forEach);
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        KeyedAddInputsExample.class.getName());
  }
}
