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
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;


public class SReduceExample extends StreamingTsetExample {
  private static final long serialVersionUID = -275307275783819805L;
  private static final Logger LOG = Logger.getLogger(SReduceExample.class.getName());

  @Override
  public void buildGraph(StreamingTSetEnvironment env) {
    SSourceTSet<Integer> src = dummySource(env, 8, PARALLELISM);


    SDirectTLink<Integer> link = src.direct();

//    link.map(i -> i * 2).direct().forEach(i -> LOG.info("m" + i.toString()));
//
//    link.flatmap((i, c) -> c.collect("fm" + i))
//        .direct().forEach(i -> LOG.info(i.toString()));
//
//    link.compute(i -> i + "C")
//        .direct().forEach(i -> LOG.info(i));
//
//    link.compute((input, output) -> output.collect(input + "DD"))
//        .direct().forEach(s -> LOG.info(s.toString()));

    link.countWindow(2, input -> input)
        .direct()
        .forEach(i -> LOG.info(i.toString()));
//    link.countWindow(2, input -> input)
//        .direct()
//        .forEach(new ApplyFunc<List<IMessage<Integer>>>() {
//          @Override
//          public void apply(List<IMessage<Integer>> data) {
//            System.out.println("Window Received=> : " + data.size());
//            for (IMessage<Integer> m : data) {
//              Object o = m.getContent();
//              if (o instanceof List) {
//                ArrayList<?> x = (ArrayList<?>) o;
//                System.out.println("s1: " + x.size());
//                for (Object o1 : x
//                ) {
//                  IMessage<Integer> it = (IMessage<Integer>) o1;
//                  System.out.println("s3 : " + o1.getClass().getName() + ", "
//                      + it.getContent().intValue());
//
//                }
//              }
//            }
//          }
//        });
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig, SReduceExample.class.getName());
  }
}
