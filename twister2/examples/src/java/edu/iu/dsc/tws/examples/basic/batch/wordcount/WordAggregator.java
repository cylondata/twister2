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
package edu.iu.dsc.tws.examples.basic.batch.wordcount;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.GatherBatchReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class WordAggregator implements GatherBatchReceiver {
  private static final Logger LOG = Logger.getLogger(WordAggregator.class.getName());

  private Config config;

  private DataFlowOperation operation;

  private int totalCount = 0;

  private Map<String, Integer> wordCounts = new HashMap<>();

  private int executor;

  @Override
  public void init(Config cfg, DataFlowOperation op,
                   Map<Integer, List<Integer>> expectedIds) {
    TaskPlan plan = op.getTaskPlan();
    this.executor = op.getTaskPlan().getThisExecutor();
    LOG.fine(String.format("%d final expected task ids %s", plan.getThisExecutor(), expectedIds));
  }

  @Override
  public void receive(int target, Iterator<Object> it) {
    Map<String, Integer> localwordCounts = new HashMap<>();
    while (it.hasNext()) {
      Object next = it.next();
      if (next instanceof List) {
        for (Object o : (List) next) {
          int count = 0;
          String value = o.toString();
          if (localwordCounts.containsKey(value)) {
            count = localwordCounts.get(value);
          }
          count++;
          totalCount++;
          localwordCounts.put(value, count);
        }
      }
    }
    LOG.info(String.format("%d Final word %s", executor, localwordCounts));
  }
}
