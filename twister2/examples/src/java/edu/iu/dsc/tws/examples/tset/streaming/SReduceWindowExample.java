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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.examples.tset.batch.BatchTsetExample;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.WindowComputeFunc;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SSourceTSet;
import edu.iu.dsc.tws.tset.sets.streaming.WindowComputeTSet;

public class SReduceWindowExample extends StreamingTsetExample {
  private static final long serialVersionUID = -2753072757838198105L;
  private static final Logger LOG = Logger.getLogger(SReduceWindowExample.class.getName());

  private static final boolean IS_COUNT_WINDOW = false;
  private static final boolean IS_WINDOW_FUNCTION_WITH_END = false;
  private static final boolean IS_WINDOW_FUNCTION_WITH_DOWNSTREAM = false;
  private static final boolean IS_WINDOW_FUNCTION_WITH_WINDOWEDREDUCE = false;
  private static final boolean IS_WINDOW_FUNCTION_WITH_WINDOW_PROCESS_USABLE = true;


  @Override
  public void buildGraph(StreamingTSetEnvironment env) {
    SSourceTSet<Integer> src = dummySource(env, 8, PARALLELISM);

    SDirectTLink<Integer> link = src.direct();

    if (IS_COUNT_WINDOW) {
      WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> windowComputeTSet
          = link
          .countWindow(2, (ComputeFunc<Iterator<Integer>, Iterator<Integer>>) input -> input);

      windowComputeTSet.direct()
          .forEach((ApplyFunc<Iterator<Integer>>) data -> {
            while (data.hasNext()) {
              System.out.print(data.next() + ", ");
            }
            System.out.println();
          });

    }

    if (IS_WINDOW_FUNCTION_WITH_END) {

      WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> itrTSet = link.countWindow(2,
          (WindowComputeFunc<Iterator<Integer>, Iterator<Integer>>) input -> {
            List<Integer> items = new ArrayList<Integer>();
            while (input.hasNext()) {
              items.add(input.next() * 2);
            }
            return items.iterator();
          });

      itrTSet.direct().forEach((ApplyFunc<Iterator<Integer>>) data -> {
        while (data.hasNext()) {
          System.out.print(data.next() + ", ");
        }
        System.out.println();
      });

    }

    if (IS_WINDOW_FUNCTION_WITH_DOWNSTREAM) {

      WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> itrTSet = link.countWindow(2,
          (WindowComputeFunc<Iterator<Integer>, Iterator<Integer>>) input -> {
            List<Integer> items = new ArrayList<Integer>();
            while (input.hasNext()) {
              items.add(input.next() * 2);
            }
            return items.iterator();
          });

      SComputeTSet<Integer, Iterator<Integer>> mapTSet = itrTSet
          .direct()
          .map((MapFunc<Integer, Iterator<Integer>>) input -> {
            Integer sum = 0;
            while (input.hasNext()) {
              sum += input.next();
            }
            return sum;
          });

      mapTSet.direct().forEach((ApplyFunc<Integer>) data -> System.out.println(data));
    }

    if (IS_WINDOW_FUNCTION_WITH_WINDOWEDREDUCE) {

      link.countWindow(2, (MapFunc<Integer, Iterator<Integer>>) input -> {
        Integer sum = 0;
        while (input.hasNext()) {
          sum += input.next();
        }
        return sum;
      });

      //link.countWindow().reduce(a,b-> a + b)

    }

    if (IS_WINDOW_FUNCTION_WITH_WINDOW_PROCESS_USABLE) {

      WindowComputeTSet<Integer, Iterator<Integer>> winTSet = link.countWindow(2);
      WindowComputeTSet<Integer, Iterator<Integer>> processedTSet = winTSet
          .process((WindowComputeFunc<Integer, Iterator<Integer>>) input -> {
            Integer sum = 0;
            while (input.hasNext()) {
              sum += input.next();
            }
            return sum;
          });

      processedTSet.direct().forEach((ApplyFunc<Integer>) data -> System.out.println(data));

      //link.countWindow().reduce(a,b-> a + b)

    }

  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        SReduceWindowExample.class.getName());
  }
}