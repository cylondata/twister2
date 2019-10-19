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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.fn.BaseComputeFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class AddInputsExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(AddInputsExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    SourceTSet<Integer> src1 = dummySourceOther(env, COUNT, PARALLELISM);

    CachedTSet<Integer> cache1 = src1.direct().cache();
    CachedTSet<Integer> cache = src.direct().cache().addInput("foo", cache1);

    CachedTSet<Integer> out = cache.direct().compute(
        new BaseComputeFunc<Integer, Iterator<Integer>>() {
          @Override
          public Integer compute(Iterator<Integer> input) {
            DataPartitionConsumer<?> c1 = getTSetContext().getInput("foo").getConsumer();

            int out = 0;
            while (input.hasNext() && c1.hasNext()) {
              out += input.next() + (Integer) (c1.next());
            }

            return out;
          }
        }
    ).lazyCache();

    for (int i = 0; i < 4; i++) {
      env.eval(out);
    }

    out.direct().forEach(l -> LOG.info(l.toString()));

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        AddInputsExample.class.getName());
  }
}
