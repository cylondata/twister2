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
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class AllreduceExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(AllreduceExample.class.getName());

  private static final int COUNT = 10;
  private static final int PARALLELISM = 4;
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    LOG.info("Test foreach");
    src.allReduce(Integer::sum)
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("Test map");
    src.allReduce(Integer::sum)
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("Test flat map");
    src.allReduce(Integer::sum)
        .flatmap((i, c) -> c.collect(i.toString() + "$$"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("Test compute");
    src.allReduce(Integer::sum)
        .compute(i -> i * 2)
        .direct()
        .forEach(i -> LOG.info("comp: " + i));

    LOG.info("Test computec");
    src.allReduce(Integer::sum)
        .compute((ComputeCollectorFunc<String, Integer>)
            (input, output) -> output.collect("sum=" + input))
        .direct()
        .forEach(s -> LOG.info("computec: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, AllreduceExample.class.getName());
  }
}
