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
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class BranchingExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(BranchingExample.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    int para = 2;
    SourceTSet<Integer> src = dummySource(env, COUNT, para).setName("src0");

    KeyedTSet<Integer, Integer> left = src.mapToTuple(i -> new Tuple<>(i % 2, i)).setName("left");

    KeyedTSet<Integer, Integer> right = src.mapToTuple(i -> new Tuple<>(i % 2, i + 1)).setName(
        "right");

    JoinTLink<Integer, Integer, Integer> join = left.join(right,
        CommunicationContext.JoinType.INNER, Integer::compareTo).setName("join");


    ComputeTSet<String, Iterator<JoinedTuple<Integer, Integer, Integer>>> map
        = join.map(t -> "(" + t.getKey() + " " + t.getLeftValue() + " " + t.getRightValue() + ")")
        .setName("map***");

    ComputeTSet<String, Iterator<String>> map1 = map.direct().map(s -> "###" + s).setName("map@@");

    ComputeTSet<String, Iterator<String>> union = map.union(map1).setName("union");

    union.direct().forEach(s -> LOG.info(s));
  }

  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, BranchingExample.class.getName());
  }
}
