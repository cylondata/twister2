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
package edu.iu.dsc.tws.examples.basic.streaming.wordcount;

import java.util.List;
import java.util.Random;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

public class GenerateWordMap implements Runnable {
  private DataFlowOperation operation;

  private Random random = new Random();

  private char[] tempCharacters;

  private static final int MAX_CHARS = 100;

  private int noOfWords;

  private int noOfDestinations;

  private int taskId;

  private List<Integer> destinations;

  private RandomString randomString;

  public GenerateWordMap(Config config, DataFlowOperation operation, int words,
                         List<Integer> dests, int tId) {
    this.operation = operation;
    this.tempCharacters = new char[MAX_CHARS];
    this.noOfWords = words;
    this.destinations = dests;
    this.taskId = tId;
    this.noOfDestinations = destinations.size();
    this.randomString = new RandomString(MAX_CHARS, new Random(), RandomString.ALPHANUM);
  }

  @Override
  public void run() {
    for (int i = 0; i < noOfWords; i++) {
      String word = generateWord();
      int destIndex = Math.abs(word.hashCode() % noOfDestinations);
      int dest = destinations.get(destIndex);
      // lets try to process if send doesn't succeed
      while (!operation.send(taskId, word, 0, dest)) {
        operation.progress();
      }
    }
  }

  private String generateWord() {
    return randomString.nextRandomSizeString();
  }
}
