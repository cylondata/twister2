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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.comms.shuffle.ResettableIterator;

public final class HashJoinUtils {

  private HashJoinUtils() {

  }

  public static List<Object> rightOuterJoin(List<Tuple> leftRelation,
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
      boolean matched = false;
      for (Tuple leftTuple : leftTuples) {
        if (comparator.compare(leftTuple, rightTuple) == 0) {
          joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
              rightTuple.getValue()));
          matched = true;
        }
      }

      if (!matched) {
        joinedTuples.add(JoinedTuple.of(rightTuple.getKey(), null, rightTuple.getValue()));
      }
    }
    return joinedTuples;
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

  /**
   * Disk based inner join
   */
  public static Iterator<JoinedTuple> innerJoin(ResettableIterator<Tuple<?, ?>> leftIt,
                                                ResettableIterator<Tuple<?, ?>> rightIt,
                                                KeyComparatorWrapper comparator) {
    final double amountToKeepFree = Runtime.getRuntime().maxMemory() * 0.75;
    ResettableIterator<Tuple<?, ?>> rightItRef = rightIt;
    return new Iterator<JoinedTuple>() {

      private boolean hashingDone;
      private Map<Object, List> keyHash = new HashMap<>();

      private JoinedTuple nextJoinTuple;


      private void doHashing() {
        this.keyHash.clear();
        // building the hash, as long as memory permits
        while (Runtime.getRuntime().freeMemory() < amountToKeepFree && leftIt.hasNext()) {
          Tuple<?, ?> nextLeft = leftIt.next();
          keyHash.computeIfAbsent(nextLeft.getKey(), k -> new ArrayList()).add(nextLeft.getValue());
        }
        hashingDone = !leftIt.hasNext();

        if (!hashingDone && this.keyHash.isEmpty()) {
          // problem!. We have cleared the old hash, yet there's no free memory available to proceed
          throw new Twister2RuntimeException("Couldn't progress due to memory limitations");
        }
      }

      {
        // initially do hashing & probing
        doHashing();
        doProbing();
      }

      private Tuple<?, ?> nextRightTuple;
      private List leftListForCurrentKey;
      private int leftListIndex = 0;

      /**
       * This method should be guaranteed to create a {@link JoinedTuple}
       */
      private void progressProbing() {
        JoinedTuple joinedTuple = JoinedTuple.of(
            this.nextRightTuple.getKey(),
            leftListForCurrentKey.get(leftListIndex),
            this.nextRightTuple.getValue()
        );
        leftListIndex++;
        if (leftListIndex == leftListForCurrentKey.size()) {
          nextRightTuple = null;
          leftListForCurrentKey = null;
        }
      }

      private void doProbing() {
        while (this.nextJoinTuple == null) {
          if (this.nextRightTuple == null) {
            if (rightIt.hasNext()) {
              this.nextRightTuple = rightIt.next();
              this.leftListForCurrentKey = this.keyHash.get(nextRightTuple.getKey());
              if (this.leftListForCurrentKey == null) {
                // not left tuples from left relation to join, skipping this right tuple
                this.nextRightTuple = null;
              } else {
                progressProbing();
              }
            } else {
              // right iterator has reached to an end.
              if (!hashingDone) {
                // clear current hash and reset the right iterator
                doHashing();
                rightIt.reset();
              } else {
                // end of join operation
                break;
              }
            }
          } else {
            progressProbing();
          }
        }
      }

      @Override
      public boolean hasNext() {
        return this.nextJoinTuple != null;
      }

      @Override
      public JoinedTuple next() {
        if (!hasNext()) {
          throw new Twister2RuntimeException("Join operation has reached to an end. "
              + "Use hasNext() to check the status.");
        }
        JoinedTuple currentJoinTuple = nextJoinTuple;
        doProbing();
        return currentJoinTuple;
      }
    };
  }
}
