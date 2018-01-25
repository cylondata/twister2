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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.examples.utils.RandomString;

public class BatchWordSource implements Runnable {
  private static final Logger LOG = Logger.getLogger(BatchWordSource.class.getName());

  private DataFlowOperation operation;

  private Random random = new Random();

  private char[] tempCharacters;

  private static final int MAX_CHARS = 5;

  private int noOfWords;

  private int noOfDestinations;

  private int taskId;

  private List<Integer> destinations;

  private RandomString randomString;

  private int noOfWordsSent;

  private int executor;

  private List<String> sampleWords = new ArrayList<>();

  public BatchWordSource(Config config, DataFlowOperation operation, int words,
                         List<Integer> dests, int tId, int noOfSampleWords) {
    this.operation = operation;
    this.tempCharacters = new char[MAX_CHARS];
    this.noOfWords = words;
    this.destinations = dests;
    this.taskId = tId;
    this.noOfDestinations = destinations.size();
    this.randomString = new RandomString(MAX_CHARS, new Random(), RandomString.ALPHANUM);
    this.executor = operation.getTaskPlan().getThisExecutor();

    for (int i = 0; i < noOfSampleWords; i++) {
      sampleWords.add(randomString.nextRandomSizeString());
    }
  }

  @Override
  public void run() {
    int nextIndex = 0;
    for (int i = 0; i < noOfWords; i++) {
      String word = sampleWords.get(random.nextInt(sampleWords.size()));
      int destIndex = Math.abs(word.hashCode() % noOfDestinations);
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
//      LOG.info(String.format("%d %d Sending word true %d", executor, taskId, noOfWordsSent));
    }
  }

  private String generateWord() {
    return randomString.nextRandomSizeString();
  }
}
