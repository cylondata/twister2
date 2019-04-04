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

public final class IntIteratorComparator implements ResultsComparator<Iterator<int[]>> {

  private static final IntIteratorComparator INSTANCE = new IntIteratorComparator();
  private IntArrayComparator intArrayComparator = IntArrayComparator.getInstance();

  private IntIteratorComparator() {

  }

  public static IntIteratorComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean compare(Iterator<int[]> d1, Iterator<int[]> d2) {
    List<int[]> d1List = new ArrayList<>();
    while (d1.hasNext()) {
      d1List.add(d1.next());
    }

    outer:
    while (d2.hasNext()) {
      int[] d2Data = d2.next();
      Iterator<int[]> d1Itr = d1List.iterator();
      while (d1Itr.hasNext()) {
        boolean match = this.intArrayComparator.compare(d2Data, d1Itr.next());
        if (match) {
          d1Itr.remove();
          continue outer;
        }
      }
      //couldn't find matching element
      return false;
    }
    return d1List.isEmpty();
  }
}
