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
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.SDirectTLink;
import edu.iu.dsc.tws.api.tset.sets.streaming.SSourceTSet;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;


public class SDirectExample extends StreamingTsetExample {
  private static final Logger LOG = Logger.getLogger(SDirectExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  @Override
  public void execute(StreamingTSetEnvironment env) {
    SSourceTSet<Integer> src = dummySource(env, COUNT, PARALLELISM);

    SDirectTLink<Integer> direct = src.direct();

    direct.map(i -> i * 2)
        .direct()
        .flatmap((FlatMapFunc<Integer, Integer>) (i, c) -> c.collect(i * 10))
        .direct()
        .compute((ComputeFunc<String, Integer>) i -> i.toString() + "C")
        .direct()
        .compute((ComputeCollectorFunc<String, String>)
            (input, output) -> output.collect(input + "DD"))
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

/*    LOG.info("test sink");
    direct.sink((SinkFunc<Integer>) value -> {
      LOG.info("val =" + value);
      return true;
    });*/

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, SDirectExample.class.getName());
  }
}
