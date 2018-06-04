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

import java.util.List;
import java.util.Random;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;

public class RecordSource implements Runnable {
  private DataFlowOperation operation;

  private Random random = new Random();

  private char[] tempCharacters;

  private static final int MAX_CHARS = 5;

  private int noOfWords;

  private int noOfDestinations;

  private int taskId;

  private List<Integer> destinations;

  private RecordGenerator randomString;

  private int noOfWordsSent;

  private int executor;

  private RecordGenerator recordGenerator;

  public RecordSource(Config config, DataFlowOperation operation,
                      List<Integer> dests, int tIndex,
                      int noOfRecords, int range, int totalTasks) {
    this.operation = operation;
    this.tempCharacters = new char[MAX_CHARS];
    this.noOfWords = noOfRecords;
    this.destinations = dests;
    this.taskId = tIndex;
    this.noOfDestinations = destinations.size();
    this.executor = operation.getTaskPlan().getThisExecutor();
    this.recordGenerator = new RecordGenerator(tIndex, range, totalTasks);
  }

  @Override
  public void run() {
    int nextIndex = 0;
    for (int i = 0; i < noOfWords; i++) {
      Record word = recordGenerator.next();
      nextIndex = nextIndex % noOfDestinations;
      int dest = destinations.get(nextIndex);
      nextIndex++;
      int flags = 0;
      if (i >= (noOfWords - noOfDestinations)) {
        flags = MessageFlags.FLAGS_LAST;
      }
      // lets try to process if send doesn't succeed
      while (!operation.send(taskId, word, flags, dest)) {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      noOfWordsSent++;
    }
  }
}
