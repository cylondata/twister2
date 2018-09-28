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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.op.batch.BKeyedPartition;

public class RecordSource implements Runnable {
  private static final Logger LOG = Logger.getLogger(RecordSource.class.getName());

  private BKeyedPartition operation;

  private int noOfWords;

  private int taskId;

  private int executor;

  private RecordGenerator recordGenerator;

  private boolean isDone;

  public RecordSource(int workerId, BKeyedPartition op,
                      int tIndex, int noOfRecords, int range) {
    this.operation = op;
    this.noOfWords = noOfRecords;
    this.taskId = tIndex;
    this.executor = workerId;
    this.recordGenerator = new RecordGenerator(range);
  }

  @Override
  public void run() {
    try {
      for (int i = 0; i < noOfWords; i++) {
        Record word = recordGenerator.next();
        int flags = 0;

        // lets try to process if send doesn't succeed
        while (!operation.partition(taskId, word.getKey(), word.getData(), flags)) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "Error: " + executor, t);
    }
    operation.finish(taskId);
    isDone = true;
  }

  public boolean isDone() {
    return isDone;
  }
}
