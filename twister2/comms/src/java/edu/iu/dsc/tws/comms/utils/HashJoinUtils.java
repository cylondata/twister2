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
package edu.iu.dsc.tws.comms.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;

public final class HashJoinUtils {

  private HashJoinUtils() {

  }

  public static List<Object> leftOuterJoin(List<Tuple> leftRelation,
                                           List<Tuple> rightRelation,
                                           KeyComparatorWrapper comparator) {
    Map<Object, List<Tuple>> rightHash = new HashMap<>();

    List<Object> joinedTuples = new ArrayList<>();

    for (Tuple tuple : rightRelation) {
      rightHash.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>())
          .add(tuple);
    }

    for (Tuple leftTuple : leftRelation) {
      List<Tuple> rightTuples = rightHash.getOrDefault(leftTuple.getKey(), Collections.emptyList());
      boolean matched = false;
      for (Tuple rightTuple : rightTuples) {
        if (comparator.compare(leftTuple, rightTuple) == 0) {
          joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
              rightTuple.getValue()));
          matched = true;
        }
      }

      if (!matched) {
        joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(), null));
      }
    }
    return joinedTuples;
  }

  public static List<Object> innerJoin(List<Tuple> leftRelation,
                                       List<Tuple> rightRelation,
                                       KeyComparatorWrapper comparator) {
    Map<Object, List<Tuple>> leftHash = new HashMap<>();

    List<Object> joinedTuples = new ArrayList<>();

    for (Tuple tuple : leftRelation) {
      leftHash.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>())
          .add(tuple);
    }

    for (Tuple rightTuple : rightRelation) {
      List<Tuple> leftTuples = leftHash.getOrDefault(rightTuple.getKey(), Collections.emptyList());
      for (Tuple leftTuple : leftTuples) {
        if (comparator.compare(leftTuple, rightTuple) == 0) {
          joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
              rightTuple.getValue()));
        }
      }
    }
    return joinedTuples;
  }
}
