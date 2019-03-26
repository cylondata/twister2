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
import java.util.Iterator;
import java.util.List;

import edu.iu.dsc.tws.examples.verification.ResultsComparator;

public class IteratorComparator<V> implements ResultsComparator<Iterator<V>> {

  private ResultsComparator<V> valueComparator;

  public IteratorComparator(ResultsComparator<V> valueComparator) {
    this.valueComparator = valueComparator;
  }

  @Override
  public boolean compare(Iterator<V> d1, Iterator<V> d2) {
    List<V> d1ValuesList = new ArrayList<>();

    while (d1.hasNext()) {
      d1ValuesList.add(d1.next());
    }

    while (d2.hasNext()) {
      V nextFromD2 = d2.next();
      Iterator<V> d1Iterator = d1ValuesList.iterator();
      boolean foundMatch = false;
      while (d1Iterator.hasNext()) {
        V nextFromD1 = d1Iterator.next();
        if (this.valueComparator.compare(nextFromD1, nextFromD2)) {
          d1Iterator.remove();
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        return false;
      }
    }
    return d1ValuesList.isEmpty();
  }
}
