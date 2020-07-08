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
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherUngroupedTLink;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class KGatherUngroupedExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(KGatherUngroupedExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    KeyedGatherUngroupedTLink<Integer, Integer> klink = src.mapToTuple(i -> new Tuple<>(i % 4, i))
        .keyedGatherUngrouped();

    LOG.info("test foreach");
    klink.forEach((ApplyFunc<Tuple<Integer, Integer>>)
        data -> LOG.info(data.getKey() + " -> " + data.getValue())
    );

    LOG.info("test map");
    klink.map((MapFunc<String, Tuple<Integer, Integer>>)
        input -> input.getKey() + " -> " + input.getValue())
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test compute");
    klink.compute((ComputeFunc<String, Iterator<Tuple<Integer, Integer>>>)
        input -> {
          StringBuilder sb = new StringBuilder();
          while (input.hasNext()) {
            Tuple<Integer, Integer> next = input.next();
            sb.append("[").append(next.getKey()).append("->").append(next.getValue()).append("]");
          }
          return sb.toString();
        })
        .direct()
        .forEach(s -> LOG.info("compute: " + s));

    LOG.info("test computec");
    klink.compute((ComputeCollectorFunc<String, Iterator<Tuple<Integer, Integer>>>)
        (input, output) -> {
          while (input.hasNext()) {
            Tuple<Integer, Integer> next = input.next();
            output.collect(next.getKey() + " -> " + next.getValue() * 2);
          }
        })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        KGatherUngroupedExample.class.getName());
  }
}
