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
package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;

/**
 * Final receiver for keyed gather
 */
public class KGatherBatchFinalReceiver extends KeyedReceiver {
  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  public KGatherBatchFinalReceiver(BulkReceiver receiver,
                                   int limitPerKey) {
    this.bulkReceiver = receiver;
    this.limitPerKey = limitPerKey;
    this.isFinalBatchReceiver = true;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    for (int target : messages.keySet()) {

      if (batchDone.get(target)) {
        continue;
      }

      sourcesFinished = isSourcesFinished(target);
      if (!sourcesFinished && !(dataFlowOperation.isDelegeteComplete()
          && messages.get(target).isEmpty())) {
        needsFurtherProgress = true;
      }

      if (sourcesFinished && dataFlowOperation.isDelegeteComplete()) {
        batchDone.put(target, true);
        //TODO: check if we can simply remove the data, that is use messages.remove()
        bulkReceiver.receive(target, new GatherIterator(messages.get(target)));
      }


    }

    return needsFurtherProgress;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class GatherIterator<T extends Pair<Object, Object[]>>
      implements Iterator<Pair<Object, Object[]>> {

    private Map<Object, Queue<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    GatherIterator(Map<Object, Queue<Object>> messageMap) {
      this.messageMap = messageMap;
      keyList.addAll(messageMap.keySet());
    }

    @Override
    public boolean hasNext() {
      return !keyList.isEmpty();
    }

    @Override
    public ImmutablePair<Object, Object[]> next() {
      Object key = keyList.poll();
      Object value = messageMap.remove(key);
      return new ImmutablePair(key, ((Queue) value).toArray());
    }
  }
}
