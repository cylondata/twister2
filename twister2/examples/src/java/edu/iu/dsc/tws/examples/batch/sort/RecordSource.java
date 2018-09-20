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
package edu.iu.dsc.tws.examples.batch.sort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;

public class RecordSource implements Runnable {
  private static final Logger LOG = Logger.getLogger(RecordSource.class.getName());

  private DataFlowOperation operation;

  private static final int MAX_CHARS = 5;

  private int noOfWords;

  private int noOfDestinations;

  private int taskId;

  private List<Integer> destinations;

  private int executor;

  private RecordGenerator recordGenerator;

  private List<Integer> partitioning;

  private Map<Integer, Integer> totalSends = new HashMap<>();

  public RecordSource(Config config, DataFlowOperation operation,
                      List<Integer> dests, int tIndex,
                      int noOfRecords, int range, int totalTasks) {
    this.operation = operation;
    this.noOfWords = noOfRecords;
    this.destinations = dests;
    this.taskId = tIndex;
    this.noOfDestinations = destinations.size();
    this.executor = operation.getTaskPlan().getThisExecutor();
    this.recordGenerator = new RecordGenerator(range);

    this.partitioning = new ArrayList<>();
    int perTask = range / totalTasks;
    int sum = 0;
    for (int i = 0; i < totalTasks; i++) {
      partitioning.add(sum);
      sum += perTask;
    }
    for (Integer d : dests) {
      totalSends.put(d, 0);
    }
  }

  @Override
  public void run() {
    for (int i = 0; i < noOfWords; i++) {
      Record word = recordGenerator.next();

      int destIndex = 0;
      int val = word.getKey();
      for (int j = 0; j < partitioning.size() - 1; j++) {
        if (val > partitioning.get(j) && val <= partitioning.get(j + 1)) {
          destIndex = j;
        }
      }

      int dest = destinations.get(destIndex);

      int flags = 0;
//      if (i >= (noOfWords - noOfDestinations)) {
//        flags = MessageFlags.FLAGS_LAST;
//      }

      int total = totalSends.get(dest);
      total += 1;
      totalSends.put(dest, total);

//      LOG.log(Level.INFO, String.format("%d Sending message to %d %d", executor, taskId, dest));
      // lets try to process if send doesn't succeed
      while (!operation.send(taskId, new KeyedContent(word.getKey(), word.getData(),
          MessageType.INTEGER, MessageType.BYTE), flags, dest)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    LOG.log(Level.INFO, String.format("%d total sends %s", executor, totalSends));
    operation.finish(taskId);
  }
}
