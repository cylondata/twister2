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

package edu.iu.dsc.tws.examples.tset.basic;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.link.KeyedGatherTLink;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class KGatherExample extends BaseTsetExample {
  private static final Logger LOG = Logger.getLogger(KGatherExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    BatchSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    KeyedGatherTLink<Integer, Integer> klink = src.mapToTuple(i -> new Tuple<>(i % 4, i))
        .keyedGather();

    LOG.info("test foreach");
    klink.forEach((ApplyFunc<Tuple<Integer, Iterator<Integer>>>)
        data -> LOG.info("key " + data.getKey() + " " + data.getValue().toString())
    );

    LOG.info("test map");
    klink.map((MapFunc<String, Tuple<Integer, Iterator<Integer>>>)
        input -> {
          int s = 0;
          while (input.getValue().hasNext()) {
            s += input.getValue().next();
          }
          return "key " + input.getKey() + " " + s;
        })
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test compute");
    klink.compute((ComputeFunc<String, Iterator<Tuple<Integer, Iterator<Integer>>>>)
        input -> {
          StringBuilder s = new StringBuilder();
          while (input.hasNext()) {
            Tuple<Integer, Iterator<Integer>> next = input.next();
            s.append("$").append(next.getKey()).append("_").append(next.getValue().toString());
          }
          return s.toString();
        })
        .direct()
        .forEach(s -> LOG.info("compute: concat " + s));

    LOG.info("test computec");
    klink.compute((ComputeCollectorFunc<String, Iterator<Tuple<Integer, Iterator<Integer>>>>)
        (input, output) -> {
          while (input.hasNext()) {
            Tuple<Integer, Iterator<Integer>> next = input.next();
            output.collect(next.getKey() + "#" + next.getValue().toString());
          }
        })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig, KGatherExample.class.getName());
  }
}
