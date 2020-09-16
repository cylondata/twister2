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
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;


public class SReduceExample extends StreamingTsetExample {
  private static final long serialVersionUID = -275307275783819805L;
  private static final Logger LOG = Logger.getLogger(SReduceExample.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    StreamingEnvironment env = TSetEnvironment.initStreaming(workerEnv);
    SSourceTSet<Integer> src = dummySource(env, 8, PARALLELISM);

    SDirectTLink<Integer> link = src.direct();

    link.map(i -> i * 2).direct().forEach(i -> LOG.info("m" + i.toString()));

    link.flatmap((i, c) -> c.collect("fm" + i))
        .direct().forEach(i -> LOG.info(i.toString()));

    link.compute(i -> i + "C")
        .direct().forEach(i -> LOG.info(i));

    link.mapToTuple(i -> new Tuple<>(i, i.toString()))
        .keyedDirect()
        .forEach(i -> LOG.info("mapToTuple: " + i.toString()));

    link.compute((input, output) -> output.collect(input + "DD"))
        .direct().forEach(s -> LOG.info(s.toString()));

    // Runs the entire TSet graph
    env.run();
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, SReduceExample.class.getName());
  }
}
