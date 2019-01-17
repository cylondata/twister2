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
package edu.iu.dsc.tws.examples.batch.wordcount.comms;

import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;

public class WordAggregator implements BulkReceiver {
  private static final Logger LOG = Logger.getLogger(WordAggregator.class.getName());

  private boolean isDone;

  @Override
  public void init(Config cfg, Set<Integer> expectedIds) {
    isDone = false;
  }

  @SuppressWarnings("rawtypes")
  public boolean receive(int target, Iterator<Object> it) {
    while (it.hasNext()) {
      Object next = it.next();
      if (next instanceof ImmutablePair) {
        ImmutablePair kc = (ImmutablePair) next;
        LOG.log(Level.INFO, String.format("%d Word %s count %s",
            target, kc.getKey(), ((int[]) kc.getValue())[0]));
      }
    }
    isDone = true;

    return true;
  }

  public boolean isDone() {
    return isDone;
  }
}
