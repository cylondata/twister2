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
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class CacheExample extends BaseTsetExample {
  private static final Logger LOG = Logger.getLogger(CacheExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(TSetEnvironment env) {
    BatchSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    CachedTSet<Integer> cache = src.direct().cache();

    // test cache.direct() which has IterLink semantics

    LOG.info("test foreach");
    cache.direct()
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    cache.direct()
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flat map");
    cache.direct()
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test compute");
    cache.direct()
        .compute((ComputeFunc<String, Iterator<Integer>>) input -> {
          int sum = 0;
          while (input.hasNext()) {
            sum += input.next();
          }
          return "sum" + sum;
        })
        .direct()
        .forEach(i -> LOG.info("comp: " + i));

    LOG.info("test computec");
    cache.direct()
        .compute((ComputeCollectorFunc<String, Iterator<Integer>>)
            (input, output) -> {
              int sum = 0;
              while (input.hasNext()) {
                sum += input.next();
              }
              output.collect("sum" + sum);
            })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("test sink");
    cache.direct()
        .sink((SinkFunc<Iterator<Integer>>) value -> {
          while (value.hasNext()) {
            LOG.info("val =" + value.next());
          }
          return true;
        });


    // test cache.reduce() which has SingleLink semantics

    LOG.info("test foreach");
    cache.reduce(Integer::sum)
        .forEach(i -> LOG.info("foreach: " + i));

    LOG.info("test map");
    cache.reduce(Integer::sum)
        .map(i -> i.toString() + "$$")
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test flat map");
    cache.reduce(Integer::sum)
        .flatmap((i, c) -> c.collect(i.toString() + "##"))
        .direct()
        .forEach(s -> LOG.info("flat:" + s));

    LOG.info("test compute");
    cache.reduce(Integer::sum)
        .compute((ComputeFunc<String, Integer>)
            input -> "sum=" + input)
        .direct()
        .forEach(s -> LOG.info("compute: " + s));

    LOG.info("test computec");
    cache.reduce(Integer::sum)
        .compute((ComputeCollectorFunc<String, Integer>)
            (input, output) -> output.collect("sum=" + input))
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    LOG.info("test sink");
    cache.reduce(Integer::sum).sink((SinkFunc<Integer>)
        value -> {
          LOG.info("val =" + value);
          return true;
        });

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig, CacheExample.class.getName());
  }
}