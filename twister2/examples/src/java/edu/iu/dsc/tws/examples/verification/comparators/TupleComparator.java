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

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.examples.verification.ResultsComparator;

public class TupleComparator<K, V> implements ResultsComparator<Tuple<K, V>> {

  private ResultsComparator<K> keyComparator;
  private ResultsComparator<V> valueComparator;

  public TupleComparator(ResultsComparator<K> keyComparator,
                         ResultsComparator<V> valueComparator) {

    this.keyComparator = keyComparator;
    this.valueComparator = valueComparator;
  }

  @Override
  public boolean compare(Tuple<K, V> d1, Tuple<K, V> d2) {
    return keyComparator.compare(d1.getKey(), d2.getKey())
        && valueComparator.compare(d1.getValue(), d2.getValue());
  }
}
