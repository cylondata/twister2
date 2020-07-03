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
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.links.batch.KeyedDirectTLink;
import edu.iu.dsc.tws.tset.sets.batch.KeyedCachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;

public class KeyedOperationsExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(KeyedOperationsExample.class.getName());

  @Override
  public void execute(BatchEnvironment env) {
    KeyedSourceTSet<String, Integer> kSource = dummyKeyedSource(env, COUNT, PARALLELISM);

    KeyedDirectTLink<String, Integer> kDirect = kSource.keyedDirect();

    kDirect.forEach(i -> LOG.info("d_fe: " + i.toString()));

    KeyedCachedTSet<String, Integer> cache = kDirect.cache();

    cache.keyedDirect().forEach(i -> LOG.info("c_d_fe: " + i.toString()));

    cache.keyedReduce(Integer::sum).forEach(i -> LOG.info("c_r_fe: " + i.toString()));

    cache.keyedGather().forEach((ApplyFunc<Tuple<String, Iterator<Integer>>>) data -> {
      StringBuilder sb = new StringBuilder();
      sb.append("c_g_fe: key ").append(data.getKey()).append("->");
      while (data.getValue().hasNext()) {
        sb.append(" ").append(data.getValue().next());
      }
      LOG.info(sb.toString());
    });
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        KeyedOperationsExample.class.getName());
  }
}
