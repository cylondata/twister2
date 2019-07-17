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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.Compute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollector;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class ReduceExample extends BaseTsetExample {
  private static final Logger LOG = Logger.getLogger(ReduceExample.class.getName());

  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(TSetEnvironment env) {
    BatchSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    ReduceTLink<Integer> reduce = src.reduce(Integer::sum);

    LOG.info("test foreach");
    reduce.forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    reduce
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flat map");
    reduce
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test compute");
    reduce
        .compute((Compute<String, Integer>)
            input -> "sum=" + input)
        .direct()
        .forEach(s -> LOG.info("compute: " + s));

    LOG.info("test computec");
    reduce
        .compute((ComputeCollector<String, Integer>)
            (input, output) -> output.collect("sum=" + input))
        .direct()
        .forEach(s -> LOG.info("computec: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig, ReduceExample.class.getName());
  }
}
