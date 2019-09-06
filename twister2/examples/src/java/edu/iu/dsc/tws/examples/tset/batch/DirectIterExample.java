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
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class DirectIterExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(DirectIterExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(BatchTSetEnvironment env) {
    SourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM).setName("src");
    LOG.info("test direct iteration");
    ComputeTSet<Integer, Iterator<Integer>> testmap = src.direct().map(input -> input);
    CachedTSet<Integer> cached = null;
    for (int i = 0; i < 4; i++) {
      cached = testmap.cache(true);
    }
    System.out.println(cached.getDataObject().getPartitions().length);
    testmap.finishIter();
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, DirectIterExample.class.getName());
  }
}
