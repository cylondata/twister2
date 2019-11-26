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
import edu.iu.dsc.tws.api.tset.fn.BaseComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class AddInputsExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(AddInputsExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    // source with 25..29
    SourceTSet<Integer> baseSrc = dummySourceOther(env, COUNT, PARALLELISM);
    // source with 0..4
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    CachedTSet<Integer> srcCache = src.direct().cache().setName("src");

    // make src an input of baseSrc
    CachedTSet<Integer> baseSrcCache = baseSrc.direct().cache().setName("baseSrc");

    CachedTSet<Integer> out = baseSrcCache.direct().compute(
        new BaseComputeCollectorFunc<Integer, Iterator<Integer>>() {
          @Override
          public void compute(Iterator<Integer> input, RecordCollector<Integer> collector) {
            DataPartitionConsumer<Integer> c1 = (DataPartitionConsumer<Integer>)
                getInput("src-input").getConsumer();

            while (input.hasNext() && c1.hasNext()) {
              collector.collect(input.next() + c1.next());
            }
          }
        }
    ).addInput("src-input", srcCache).lazyCache();

    for (int i = 0; i < 4; i++) {
      LOG.info("iter: " + i);
      env.evalAndUpdate(out, baseSrcCache);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    baseSrcCache.direct().forEach(l -> LOG.info(l.toString()));

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        AddInputsExample.class.getName());
  }
}
