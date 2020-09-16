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
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class PersistExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(PersistExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    int start = env.getWorkerID() * 100;
    SourceTSet<Integer> src = dummySource(env, start, COUNT, PARALLELISM);

    // test direct().cache() which has IterLink semantics
    PersistedTSet<Integer> cache = src.direct().persist();
    runOps(env, cache);

    // test reduce().cache() which has SingleLink semantics
    LOG.info("test persist after reduce");
    PersistedTSet<Integer> cache1 = src.reduce(Integer::sum).persist();
    runOps(env, cache1);

    // test gather.cache() which has TupleValueIterLink
    LOG.info("test persist after gather");
    PersistedTSet<Integer> cache2 = src.gather().persist();
    runOps(env, cache2);
  }

  private void runOps(BatchEnvironment env, PersistedTSet<Integer> cTset) {
    LOG.info("test foreach");
    cTset.direct()
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test list");
    List<Integer> data = cTset.getData();
    LOG.info("List out: " + data);

    LOG.info("test map");
    cTset.direct()
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flatmap");
    cTset.direct()
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test compute");
    cTset.direct()
        .compute((ComputeFunc<Iterator<Integer>, String>) input -> {
          int sum = 0;
          while (input.hasNext()) {
            sum += input.next();
          }
          return "sum" + sum;
        })
        .direct()
        .forEach(i -> LOG.info("comp: " + i));

    LOG.info("test computec");
    cTset.direct()
        .compute((ComputeCollectorFunc<Iterator<Integer>, String>)
            (input, output) -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next();
              }
              output.collect("sum" + sum);
            })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("test sink");
    SinkTSet<Iterator<Integer>> sink = cTset.direct()
        .sink((SinkFunc<Iterator<Integer>>) value -> {
          while (value.hasNext()) {
            LOG.info("sink =" + value.next());
          }
          return true;
        });
    env.run(sink);
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, PersistExample.class.getName());
  }
}
