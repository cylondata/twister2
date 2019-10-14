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
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;


public class EvalCacheExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(EvalCacheExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM).setName("src");

    CachedTSet<Integer> cache0 = src.direct().cache();
    CachedTSet<Integer> cache1 = src.direct().map(i -> i * 10).direct().cache();

    cache0.addInput("cacheIn", cache1);

    LOG.info("test foreach");
    ComputeTSet<Object, Iterator<Integer>> tset1 =
        cache0.direct().lazyForEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    ComputeTSet<Object, Iterator<String>> tset2 =
        cache0.direct().map(i -> i.toString() + "$$").setName("map")
            .direct()
            .lazyForEach(s -> LOG.info("map: " + s));

//    for (int i = 0; i < 4; i++) {
//      LOG.info("iter " + i);
//      env.eval(tset1);
//      try {
//        Thread.sleep(1000);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
////      env.eval(tset2);
//    }

    env.finishEval(tset1);
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, EvalCacheExample.class.getName());
  }
}
