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


package edu.iu.dsc.tws.examples.ntset.basic;

import java.util.HashMap;
import java.util.Iterator;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class ComputeCollectExample extends BaseTsetExample {
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(TSetEnvironment env) {
    BatchSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM).setName("src");

    ComputeTSet<String, Iterator<Integer>> modify = src.direct().compute(
        (ComputeCollectorFunc<String, Iterator<Integer>>) (input, collector) -> {
          while (input.hasNext()) {
            collector.collect(input.next() + "##");
          }
        }).setName("modify");


    modify.direct().forEach(data -> System.out.println("val: " + data));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BaseTsetExample.submitJob(config, PARALLELISM, jobConfig,
        ComputeCollectExample.class.getName());
  }
}
