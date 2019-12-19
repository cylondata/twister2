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

  private static final boolean CASE_1 = false;
  private static final boolean CASE_2 = false;
  private static final boolean CASE_3 = true;

  @Override
  public void buildGraph(StreamingTSetEnvironment env) {
    SSourceTSet<Integer> src = dummySource(env, 8, PARALLELISM);

    SDirectTLink<Integer> link = src.direct();

    if (CASE_1) {
      WindowComputeTSet<Iterator<Integer>, Iterator<Integer>> windowComputeTSet
          = link.countWindow(2, input -> input);

      windowComputeTSet.direct()
          .forEach((ApplyFunc<Iterator<Integer>>) data -> {
            while (data.hasNext()) {
              System.out.print(data.next() + ", ");
            }
            System.out.println();
          });

    }

    if (CASE_2) {

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

    if (CASE_3) {

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


  }


  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    BatchTsetExample.submitJob(config, PARALLELISM, jobConfig,
        SReduceWindowExample.class.getName());
  }
}
