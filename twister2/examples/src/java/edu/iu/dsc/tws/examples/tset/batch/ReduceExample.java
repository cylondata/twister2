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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.ReduceTLink;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class ReduceExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(ReduceExample.class.getName());

  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    int start = env.getWorkerID() * 100;
    SourceTSet<Integer> src = dummySource(env, start, COUNT, PARALLELISM);

    ReduceTLink<Integer> reduce = src.reduce(Integer::sum);

    LOG.info("test foreach");
    reduce.forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    reduce
        .map(i -> i.toString() + "$$")
        .withSchema(PrimitiveSchemas.STRING)
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flat map");
    reduce
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .withSchema(PrimitiveSchemas.STRING)
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test compute");
    reduce
        .compute((ComputeFunc<Integer, String>) input -> "sum=" + input)
        .withSchema(PrimitiveSchemas.STRING)
        .direct()
        .forEach(s -> LOG.info("compute: " + s));

    LOG.info("test computec");
    reduce
        .compute((ComputeCollectorFunc<Integer, String>)
            (input, output) -> output.collect("sum=" + input))
        .withSchema(PrimitiveSchemas.STRING)
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("test map2tup");
    reduce.mapToTuple(i -> new Tuple<>(i, i.toString()))
        .keyedDirect()
        .forEach(i -> LOG.info("mapToTuple: " + i.toString()));

    LOG.info("test sink");
    SinkTSet<Integer> sink = reduce.sink((SinkFunc<Integer>)
        value -> {
          LOG.info("val =" + value);
          return true;
        });
    env.run(sink);
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, ReduceExample.class.getName());
  }
}
