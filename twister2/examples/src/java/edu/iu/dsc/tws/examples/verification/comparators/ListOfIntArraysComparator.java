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

import java.util.Arrays;
import java.util.List;

import edu.iu.dsc.tws.examples.verification.ResultsComparator;

public final class ListOfIntArraysComparator implements ResultsComparator<List<int[]>> {

  private static final ListOfIntArraysComparator INSTANCE = new ListOfIntArraysComparator();

  private ListOfIntArraysComparator() {

  }

  public static ListOfIntArraysComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean compare(List<int[]> d1, List<int[]> d2) {
    if (d1.size() != d2.size()) {
      return false;
    }
    for (int i = 0; i < d1.size(); i++) {
      if (!Arrays.equals(d1.get(i), d2.get(i))) {
        return false;
      }
    }
    return true;
  }
}
