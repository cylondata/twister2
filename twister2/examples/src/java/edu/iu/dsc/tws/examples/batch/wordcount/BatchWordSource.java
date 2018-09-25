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
package edu.iu.dsc.tws.examples.batch.wordcount;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.op.batch.BPartition;
import edu.iu.dsc.tws.examples.utils.RandomString;

public class BatchWordSource implements Runnable {
  private static final Logger LOG = Logger.getLogger(BatchWordSource.class.getName());

  private BPartition operation;

  private Random random = new Random();

  private static final int MAX_CHARS = 5;

  private int noOfWords;

  private int taskId;

  private List<String> sampleWords = new ArrayList<>();

  private boolean done;

  public BatchWordSource(BPartition operation, int words,
                             int tId, int noOfSampleWords) {
    this.operation = operation;
    this.noOfWords = words;
    this.taskId = tId;
    RandomString randomString = new RandomString(MAX_CHARS, new Random(), RandomString.ALPHANUM);

    for (int i = 0; i < noOfSampleWords; i++) {
      sampleWords.add(randomString.nextRandomSizeString());
    }
  }

  @Override
  public void run() {
    for (int i = 0; i < noOfWords; i++) {
      String word = sampleWords.get(random.nextInt(sampleWords.size()));
      // lets try to process if send doesn't succeed
      while (!operation.partition(taskId, word, 0)) {
        operation.progress();
      }
    }
    // we need to finish the operation
    operation.finish(taskId);
    done = true;
  }

  public boolean isDone() {
    return done;
  }
}
