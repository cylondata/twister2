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
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class DirectIterExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(DirectIterExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM).setName("src");

    // cache the src input (first task graph will be created and executed)
    CachedTSet<Integer> cachedInput = src.direct().cache();

    LOG.info("test direct iteration");
    ComputeTSet<Object> lazyForEach = cachedInput.direct().lazyForEach(
        input -> {
          LOG.info("##" + input.toString());
        });

    // eval the lazyCached tset for 4 times (second task graph will be created once but
    // will be executed 4 times)
    for (int i = 0; i < 4; i++) {
      env.eval(lazyForEach);
    }
    env.finishEval(lazyForEach);
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, DirectIterExample.class.getName());
  }
}
