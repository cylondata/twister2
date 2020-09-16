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
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherTLink;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;

public class KGatherExample extends BatchTsetExample {
  private static final Logger LOG = Logger.getLogger(KGatherExample.class.getName());
  private static final long serialVersionUID = -2753072757838198105L;

  private <T> String iterToString(Iterator<T> iter) {
    StringBuilder sb = new StringBuilder();
    while (iter.hasNext()) {
      sb.append(iter.next()).append(" ");
    }
    return sb.toString();
  }

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    int start = env.getWorkerID() * 100;
    SourceTSet<Integer> src = dummySource(env, start, COUNT, PARALLELISM);

    KeyedGatherTLink<Integer, Integer> klink = src.mapToTuple(i -> new Tuple<>(i % 10, i))
        .keyedGather();

    LOG.info("test foreach");
    klink.forEach((ApplyFunc<Tuple<Integer, Iterator<Integer>>>)
        data -> LOG.info(data.getKey() + " -> " + iterToString(data.getValue()))
    );

    LOG.info("test map");
    klink.map((MapFunc<Tuple<Integer, Iterator<Integer>>, String>)
        input -> {
          int s = 0;
          while (input.getValue().hasNext()) {
            s += input.getValue().next();
          }
          return input.getKey() + " -> " + s;
        })
        .direct()
        .forEach(s -> LOG.info("map: " + s));

    LOG.info("test compute");
    klink.compute((ComputeFunc<Iterator<Tuple<Integer, Iterator<Integer>>>, String>)
        input -> {
          StringBuilder s = new StringBuilder();
          while (input.hasNext()) {
            Tuple<Integer, Iterator<Integer>> next = input.next();
            s.append(" [").append(next.getKey()).append(" -> ")
                .append(iterToString(next.getValue())).append("] ");
          }
          return s.toString();
        })
        .direct()
        .forEach(s -> LOG.info("compute: concat " + s));

    LOG.info("test computec");
    klink.compute((ComputeCollectorFunc<Iterator<Tuple<Integer, Iterator<Integer>>>, String>)
        (input, output) -> {
          while (input.hasNext()) {
            Tuple<Integer, Iterator<Integer>> next = input.next();
            output.collect(next.getKey() + " -> " + iterToString(next.getValue()));
          }
        })
        .direct()
        .forEach(s -> LOG.info("computec: " + s));

    //Test byte[] key value pairs for KeyedGather
    SourceTSet<String> srcString = dummyStringSource(env, 25, PARALLELISM);
    KeyedGatherTLink<byte[], Integer> keyedGatherLink = srcString
        .mapToTuple(s -> new Tuple<>(s.getBytes(), 1)).keyedGather();
    LOG.info("test foreach");
    keyedGatherLink.forEach((ApplyFunc<Tuple<byte[], Iterator<Integer>>>)
        data -> LOG.info(new String(data.getKey()) + " -> " + iterToString(data.getValue()))
    );

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, KGatherExample.class.getName());
  }
}
