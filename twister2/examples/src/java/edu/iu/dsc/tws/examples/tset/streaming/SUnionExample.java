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

package edu.iu.dsc.tws.examples.tset.streaming;

import java.util.HashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.api.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.SSourceTSet;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

public class SUnionExample extends StreamingTsetExample {
  private static final Logger LOG = Logger.getLogger(SUnionExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void buildGraph(StreamingTSetEnvironment env) {
//    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM).setName("src");
    SSourceTSet<Integer> src1 = dummySource(env, COUNT, PARALLELISM).setName("src1");
    SSourceTSet<Integer> src2 = dummySourceOther(env, COUNT, PARALLELISM).setName("src2");
//    src.direct().forEach(s -> LOG.info("map sssss: " + s));
    SComputeTSet<Integer, Integer> unionTSet = src1.union(src2);
    LOG.info("test source union");
    unionTSet.direct().forEach(s -> LOG.info("map: " + s));
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM * 2, jobConfig,
        SUnionExample.class.getName());
  }
}
