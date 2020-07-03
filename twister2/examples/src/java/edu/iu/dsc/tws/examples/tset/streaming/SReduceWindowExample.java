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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.fn.AggregateFunc;
import edu.iu.dsc.tws.tset.fn.WindowComputeFunc;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;
import edu.iu.dsc.tws.tset.sets.streaming.WindowComputeTSet;

public class SReduceWindowExample extends StreamingTsetExample {

  private static final long serialVersionUID = -2753072757838198105L;

  private static final Logger LOG = Logger.getLogger(SReduceWindowExample.class.getName());

  private static final int ELEMENTS_IN_STREAM = 15;

  private static final boolean COUNT_WINDOWS = false;
  private static final boolean DURATION_WINDOWS = true;

  private static final boolean REDUCE_WINDOW = false;
  private static final boolean PROCESS_WINDOW = false;


  @Override
  public void buildGraph(StreamingEnvironment env) {

    SSourceTSet<Integer> src = dummySource(env, ELEMENTS_IN_STREAM, PARALLELISM);
    SDirectTLink<Integer> link = src.direct();

    if (COUNT_WINDOWS) {

      if (PROCESS_WINDOW) {

        WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> winTSet
            = link.countWindow(2);

        WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> processedTSet = winTSet
            .process((WindowComputeFunc<Iterator<Integer>, Iterator<Integer>>) input -> {
              List<Integer> list = new ArrayList<>();
              while (input.hasNext()) {
                list.add(input.next());
              }
              return list.iterator();
            });

        processedTSet.direct().forEach((ApplyFunc<Iterator<Integer>>) data -> {
          while (data.hasNext()) {
            System.out.println(data.next());
          }

        });

      }

      if (REDUCE_WINDOW) {

        WindowComputeTSet<Integer, Iterator<Integer>> winTSet
            = link.countWindow(2);

        WindowComputeTSet<Integer, Iterator<Integer>> localReducedTSet = winTSet
            .aggregate((AggregateFunc<Integer>) Integer::sum);

        localReducedTSet.direct().forEach(System.out::println);
      }
    }

    if (DURATION_WINDOWS) {

      if (PROCESS_WINDOW) {
        System.out.println("DURATION PROCESS WINDOW");

        WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> winTSet
            = link.timeWindow(2, TimeUnit.MILLISECONDS);

        WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> processedTSet = winTSet
            .process((WindowComputeFunc<Iterator<Integer>, Iterator<Integer>>) input -> {
              List<Integer> list = new ArrayList<>();
              while (input.hasNext()) {
                list.add(input.next());
              }
              return list.iterator();
            });

        processedTSet.direct().forEach((ApplyFunc<Iterator<Integer>>) data -> {
          while (data.hasNext()) {
            System.out.println(data.next());
          }

        });

      }

      if (REDUCE_WINDOW) {

        WindowComputeTSet<Integer, Iterator<Integer>> winTSet
            = link.timeWindow(2, TimeUnit.MILLISECONDS);

        WindowComputeTSet<Integer, Iterator<Integer>> localReducedTSet = winTSet
            .aggregate((AggregateFunc<Integer>) Integer::sum);

        localReducedTSet.direct().forEach(System.out::println);

        //link.countWindow().reduce(a,b-> a + b)
      }
    }
  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        SReduceWindowExample.class.getName());
  }
}
