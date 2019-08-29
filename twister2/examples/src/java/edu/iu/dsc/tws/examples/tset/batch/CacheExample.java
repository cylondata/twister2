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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class CacheExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(CacheExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    // Test direct().cache() which has IterLink semantics
    CachedTSet<Integer> cache = src.direct().cache();
    runOps(cache);

    // Test reduce().cache() which has SingleLink semantics
    CachedTSet<Integer> cache1 = src.reduce(Integer::sum).cache();
    runOps(cache1);

    // Test gather.cache() which has TupleValueIterLink
    CachedTSet<Integer> cache2 = src.gather().cache();
    runOps(cache2);
  }

  private void runOps(CachedTSet<Integer> cache) {
    LOG.info("Test foreach");
    cache.direct()
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("Test map");
    cache.direct()
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("Test flat map");
    cache.direct()
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("Test compute");
    cache.direct()
        .compute((ComputeFunc<String, Iterator<Integer>>) input -> {
          int sum = 0;
          while (input.hasNext()) {
            sum += input.next();
          }
          return "sum" + sum;
        })
        .direct()
        .forEach(i -> LOG.info("comp: " + i));

    LOG.info("Test computec");
    cache.direct()
        .compute((ComputeCollectorFunc<String, Iterator<Integer>>)
            (input, output) -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next();
              }
              output.collect("sum" + sum);
            })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("Test sink");
    cache.direct()
        .sink((SinkFunc<Iterator<Integer>>) value -> {
          while (value.hasNext()) {
            LOG.info("val =" + value.next());
          }
          return true;
        });
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, CacheExample.class.getName());
  }
}
