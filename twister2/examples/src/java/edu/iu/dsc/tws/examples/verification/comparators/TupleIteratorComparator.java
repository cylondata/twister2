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
package edu.iu.dsc.tws.examples.verification.comparators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.verification.ResultsComparator;

public class TupleIteratorComparator<K, V> implements ResultsComparator<Iterator<Tuple<K, V>>> {

  private ResultsComparator<V> valueComparaotr;

  public TupleIteratorComparator(ResultsComparator<V> valueComparator) {
    this.valueComparaotr = valueComparator;
  }

  @Override
  public boolean compare(Iterator<Tuple<K, V>> d1, Iterator<Tuple<K, V>> d2) {
    Map<K, List<V>> taskIdDataMap = new HashMap<>();

    while (d1.hasNext()) {
      Tuple<K, V> nextTuple = d1.next();
      taskIdDataMap.computeIfAbsent(
          nextTuple.getKey(), k -> new ArrayList<>()
      ).add(nextTuple.getValue());
    }

    while (d2.hasNext()) {
      Tuple<K, V> nextTuple = d2.next();
      if (taskIdDataMap.containsKey(nextTuple.getKey())) {
        List<V> valuesForThisKey = taskIdDataMap.get(nextTuple.getKey());
        Iterator<V> iterator = valuesForThisKey.iterator();
        while (iterator.hasNext()) {
          V next = iterator.next();
          if (this.valueComparaotr.compare(
              next,
              nextTuple.getValue())) {
            iterator.remove();
            break;
          }
        }
      } else {
        return false;
      }
    }

    //if it has any non empty list, return false
    for (List<V> value : taskIdDataMap.values()) {
      if (!value.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
