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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.collect.Ordering;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public class RecordSave implements BulkReceiver {
  private static final Logger LOG = Logger.getLogger(RecordSave.class.getName());

  @Override
  public void init(Config cfg, Set<Integer> expectedIds) {
    LOG.fine(String.format("Final expected task ids %s", expectedIds));
  }

  @SuppressWarnings("Unchecked")
  @Override
  public boolean receive(int target, Iterator<Object> it) {
    int count = 0;
    List<Integer> received = new ArrayList<>();
    while (it.hasNext()) {
      Object next = it.next();
      if (next instanceof Tuple) {
        received.add((Integer) ((Tuple) next).getKey());
        count++;
      }
    }
    boolean sorted = Ordering.natural().isOrdered(received);
    LOG.info(String.format("Received message for target: %d %s", count, sorted));
    return true;
  }
}
