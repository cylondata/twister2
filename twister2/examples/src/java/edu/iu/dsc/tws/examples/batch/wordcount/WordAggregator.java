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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;

public class WordAggregator implements BulkReceiver {
  private static final Logger LOG = Logger.getLogger(WordAggregator.class.getName());

  private boolean isDone;

  @Override
  public void init(Config cfg, Set<Integer> expectedIds) {
    isDone = false;
  }

  @Override
  public boolean receive(int target, Iterator<Object> it) {
    Map<String, Integer> localwordCounts = new HashMap<>();
    while (it.hasNext()) {
      Object next = it.next();
      if (next instanceof List) {
        for (Object o : (List) next) {
          addWord(localwordCounts, o);
        }
      } else {
        addWord(localwordCounts, next);
      }
    }
    LOG.info(String.format("%d Final word %s", target, localwordCounts));
    isDone = true;
    return true;
  }

  public void addWord(Map<String, Integer> localwordCounts, Object o) {
    int count = 0;
    String value = o.toString();
    if (localwordCounts.containsKey(value)) {
      count = localwordCounts.get(value);
    }
    count++;
    localwordCounts.put(value, count);
  }

  public boolean isDone() {
    return isDone;
  }
}
