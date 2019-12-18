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
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;

public class SReduceWindowExample extends StreamingTsetExample {
  private static final long serialVersionUID = -2753072757838198105L;
  private static final Logger LOG = Logger.getLogger(SReduceWindowExample.class.getName());

  @Override
  public void buildGraph(StreamingTSetEnvironment env) {
    SSourceTSet<Integer> src = dummySource(env, 8, PARALLELISM);

    SDirectTLink<Integer> link = src.direct();

    link.countWindow(2, input -> input)
        .direct()
        .forEach(new ApplyFunc<Iterator<Integer>>() {
          @Override
          public void apply(Iterator<Integer> data) {
            while (data.hasNext()) {
              System.out.print(data.next() + ", ");
            }
            System.out.println();
          }
        });
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        SReduceWindowExample.class.getName());
  }
}
