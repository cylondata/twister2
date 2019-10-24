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
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;


public class KAddInputsExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(KAddInputsExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
//    // source with 25..29
//    SourceTSet<Integer> src0 = dummySourceOther(env, COUNT, PARALLELISM);
//    // source with 0..4
//    SourceTSet<Integer> src1 = dummySource(env, COUNT, PARALLELISM);
//
//    CachedTSet<Integer> cache0 = src0.direct().cache();
//
//    KeyedTSet<Integer, Integer> klink0 = src0.mapToTuple(i -> new Tuple<>(i, i));
//
//    klink0.addInput("input", cache0);

//    env.eval(out);
//    env.finishEval(out);
//
//    baseSrcCache.direct().forEach(l -> LOG.info(l.toString()));

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        KAddInputsExample.class.getName());
  }
}
