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
package edu.iu.dsc.tws.examples.streaming.wordcount;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;

public class WordAggregate implements SingularReceiver {
  private static final Logger LOG = Logger.getLogger(WordAggregate.class.getName());

  private int totalCount = 0;

  private Map<String, Integer> wordCounts = new HashMap<>();

  private int executor;

  @Override
  public void init(Config cfg, Set<Integer> targets) {
  }

  @Override
  public boolean receive(int target, Object message, int flags) {
    if (message instanceof KeyedContent) {
      KeyedContent kc = (KeyedContent) message;
      LOG.log(Level.INFO, String.format("%d Word %s count %s",
          target, kc.getKey(), ((int[]) kc.getValue())[0]));
    }
    return true;
  }

  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    if (object instanceof List) {
      for (Object o : (List) object) {
        addValue(o.toString());
      }
    } else {
      addValue(object.toString());
    }

    return true;
  }

  private void addValue(String value) {
    int count = 0;
    if (wordCounts.containsKey(value)) {
      count = wordCounts.get(value);
    }
    count++;
    totalCount++;
    wordCounts.put(value, count);
    if (totalCount % 100 == 0) {
      LOG.info(String.format("%d Received words: %d map: %s", executor, totalCount, wordCounts));
    }
  }
}
