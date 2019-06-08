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
package edu.iu.dsc.tws.comms.utils;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.comms.api.JoinedTuple;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public final class JoinUtils {
  private JoinUtils() {
  }

  /**
   * Inner join the left and right relation using the tuple key
   * @param leftRelation left relation
   * @param rightRelation right relation
   * @param comparator comparator
   * @return the joined relation
   */
  public static List<Object> innerJoin(List<Tuple> leftRelation, List<Tuple> rightRelation,
                                       KeyComparatorWrapper comparator) {
    int leftIndex = 0;
    int rightIndex = 0;

    leftRelation.sort(comparator);
    rightRelation.sort(comparator);

    List<Object> outPut = new ArrayList<>();
    while (leftIndex < leftRelation.size() && rightIndex < rightRelation.size()) {
      Tuple left = leftRelation.get(leftIndex);
      Tuple right = rightRelation.get(rightIndex);

      if (comparator.compare(left, right) == 0) {
        outPut.add(new JoinedTuple(left.getKey(), left.getValue(), right.getValue()));

        int index = leftIndex + 1;
        while (index < leftRelation.size()) {
          Tuple l = leftRelation.get(index);

          if (comparator.compare(l, right) == 0) {
            outPut.add(new JoinedTuple<>(l.getKey(), l.getValue(), right.getValue()));
          }
          index++;
        }

        index = rightIndex + 1;
        while (index < rightRelation.size()) {
          Tuple r = rightRelation.get(index);
          if (comparator.compare(left, r) == 0) {
            outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), r.getValue()));
          }
          index++;
        }
      } else if (comparator.compare(left, right) < 0) {
        leftIndex++;
      } else {
        rightIndex++;
      }

      leftIndex++;
      rightIndex++;
    }

    return outPut;
  }
}
