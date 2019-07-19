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

package edu.iu.dsc.tws.examples.ntset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class AllGatherExample extends BaseTsetExample {
  private static final Logger LOG = Logger.getLogger(AllGatherExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(TSetEnvironment env) {
    BatchSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    LOG.info("test foreach");
    src.allGather()
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    src.allGather().map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flat map");
    src.allGather()
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test computec");
    src.allGather()
        .compute((ComputeCollectorFunc<String, Iterator<Tuple<Integer, Integer>>>)
            (input, output) -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next().getValue();
              }
              output.collect("sum=" + sum);
            })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("test compute");
    src.allGather()
        .compute((ComputeFunc<String, Iterator<Tuple<Integer, Integer>>>)
            input -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next().getValue();
              }
              return "sum=" + sum;
            })
        .direct()
        .forEach(s -> LOG.info("compute: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig, AllGatherExample.class.getName());
  }
}
