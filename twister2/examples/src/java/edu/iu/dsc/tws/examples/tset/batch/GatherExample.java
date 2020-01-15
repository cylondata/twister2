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
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.GatherTLink;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class GatherExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(GatherExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    GatherTLink<Integer> gather = src.gather().withDataType(MessageTypes.INTEGER);

    LOG.info("test foreach");
    gather.forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    gather.map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test compute");
    gather.compute((ComputeFunc<String, Iterator<Tuple<Integer, Integer>>>) input -> {
      int sum = 0;
      while (input.hasNext()) {
        sum += input.next().getValue();
      }
      return "sum=" + sum;
    })
        .direct()
        .forEach(s -> LOG.info("compute: " + s));

    LOG.info("test computec");
    gather.compute((ComputeCollectorFunc<String, Iterator<Tuple<Integer, Integer>>>)
        (input, output) -> {
          int sum = 0;
          while (input.hasNext()) {
            sum += input.next().getValue();
          }
          output.collect("sum=" + sum);
        })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    gather.mapToTuple(i -> new Tuple<>(i % 2, i))
        .keyedDirect()
        .forEach(i -> LOG.info("mapToTuple: " + i.toString()));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, GatherExample.class.getName());
  }
}
