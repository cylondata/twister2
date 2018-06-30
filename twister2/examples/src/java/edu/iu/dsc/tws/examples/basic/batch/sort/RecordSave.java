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
package edu.iu.dsc.tws.examples.basic.batch.sort;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class RecordSave implements BatchReceiver {
  private static final Logger LOG = Logger.getLogger(RecordSave.class.getName());

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
    int count = 0;
    while (it.hasNext()) {
      Object next = it.next();
      totalCount++;
      count++;
    }
    LOG.info(String.format("Received message for targe: %d", count));
  }
}
