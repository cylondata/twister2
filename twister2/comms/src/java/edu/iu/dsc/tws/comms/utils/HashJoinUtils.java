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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.comms.shuffle.ResettableIterator;

public final class HashJoinUtils {

  private HashJoinUtils() {

  }

  public static List<Object> rightOuterJoin(List<Tuple> leftRelation,
                                            List<Tuple> rightRelation,
                                            MessageType messageType) {
    Map<Object, List<Tuple>> leftHash = new THashMap<>(messageType);

    List<Object> joinedTuples = new ArrayList<>();

    for (Tuple tuple : leftRelation) {
      leftHash.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>())
          .add(tuple);
    }

    for (Tuple rightTuple : rightRelation) {
      List<Tuple> leftTuples = leftHash.getOrDefault(rightTuple.getKey(), Collections.emptyList());
      for (Tuple leftTuple : leftTuples) {
        joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
            rightTuple.getValue()));
      }

      if (leftTuples.isEmpty()) {
        joinedTuples.add(JoinedTuple.of(rightTuple.getKey(), null, rightTuple.getValue()));
      }
    }
    return joinedTuples;
  }

  public static List<Object> leftOuterJoin(List<Tuple> leftRelation,
                                           List<Tuple> rightRelation,
                                           MessageType messageType) {
    Map<Object, List<Tuple>> rightHash = new THashMap<>(messageType);

    List<Object> joinedTuples = new ArrayList<>();

    for (Tuple tuple : rightRelation) {
      rightHash.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>())
          .add(tuple);
    }

    for (Tuple leftTuple : leftRelation) {
      List<Tuple> rightTuples = rightHash.getOrDefault(leftTuple.getKey(), Collections.emptyList());
      for (Tuple rightTuple : rightTuples) {
        joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
            rightTuple.getValue()));
      }

      if (rightTuples.isEmpty()) {
        joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(), null));
      }
    }
    return joinedTuples;
  }

  public static List<Object> innerJoin(List<Tuple> leftRelation,
                                       List<Tuple> rightRelation,
                                       MessageType messageType) {
    Map<Object, List<Tuple>> leftHash = new THashMap<>(messageType);

    List<Object> joinedTuples = new ArrayList<>();

    for (Tuple tuple : leftRelation) {
      leftHash.computeIfAbsent(tuple.getKey(), k -> new ArrayList<>())
          .add(tuple);
    }

    for (Tuple rightTuple : rightRelation) {
      List<Tuple> leftTuples = leftHash.getOrDefault(rightTuple.getKey(), Collections.emptyList());
      for (Tuple leftTuple : leftTuples) {
        joinedTuples.add(JoinedTuple.of(leftTuple.getKey(), leftTuple.getValue(),
            rightTuple.getValue()));
      }
    }
    return joinedTuples;
  }

  public static Iterator<JoinedTuple> innerJoin(ResettableIterator<Tuple<?, ?>> leftIt,
                                                ResettableIterator<Tuple<?, ?>> rightIt,
                                                MessageType keyType) {
    return join(leftIt, rightIt, CommunicationContext.JoinType.INNER, keyType);
  }

  public static Iterator<JoinedTuple> leftJoin(ResettableIterator<Tuple<?, ?>> leftIt,
                                               ResettableIterator<Tuple<?, ?>> rightIt,
                                               MessageType keyType) {
    return join(leftIt, rightIt, CommunicationContext.JoinType.LEFT, keyType);
  }

  public static Iterator<JoinedTuple> rightJoin(ResettableIterator<Tuple<?, ?>> leftIt,
                                                ResettableIterator<Tuple<?, ?>> rightIt,
                                                MessageType keyType) {
    return join(leftIt, rightIt, CommunicationContext.JoinType.RIGHT, keyType);
  }

  static class ListBasedResettableIterator implements ResettableIterator {

    private List list;
    private Iterator iterator;

    ListBasedResettableIterator(List list) {
      this.list = list;
      this.iterator = list.iterator();
    }

    @Override
    public void reset() {
      this.iterator = this.list.iterator();
    }

    @Override
    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    @Override
    public Object next() {
      return this.iterator.next();
    }
  }

  public static List<JoinedTuple> join(List<Tuple> leftRelation,
                                       List<Tuple> rightRelation,
                                       CommunicationContext.JoinType joinType,
                                       MessageType messageType) {
    Iterator<JoinedTuple> joinIterator = join(new ListBasedResettableIterator(leftRelation),
        new ListBasedResettableIterator(rightRelation), joinType, messageType);

    List<JoinedTuple> joinedTuples = new ArrayList<>();
    while (joinIterator.hasNext()) {
      joinedTuples.add(joinIterator.next());
    }

    return joinedTuples;
  }

  /**
   * Disk based inner join
   */
  public static Iterator<JoinedTuple> join(ResettableIterator<Tuple<?, ?>> leftIt,
                                           ResettableIterator<Tuple<?, ?>> rightIt,
                                           CommunicationContext.JoinType joinType,
                                           MessageType keyType) {
    // choosing hashing and probing relations
    // if inner join:
    //    hashing = left
    //    probing = right
    // if left join:
    //    hashing = right
    //    probing = left
    // if right join:
    //    hashing = left
    //    probing = right
    final ResettableIterator<Tuple<?, ?>> hashingRelation = joinType.equals(
        CommunicationContext.JoinType.LEFT) ? rightIt : leftIt;
    final ResettableIterator<Tuple<?, ?>> probingRelation = joinType.equals(
        CommunicationContext.JoinType.LEFT) ? leftIt : rightIt;

    // set the memory limits based on the heap allocation
    final double amountToKeepFree = Runtime.getRuntime().maxMemory() * 0.75;

    return new Iterator<JoinedTuple>() {

      private boolean hashingDone;
      private Map<Object, List> keyHash = new THashMap<>(keyType);

      // always keep the nextJoinTuple in memory. hasNext() will use this field
      private JoinedTuple nextJoinTuple;


      /**
       * This method will perform following actions in order
       * <ol>
       *   <li>Clear existing HashMap</li>
       *   <li>Create HashMap from the hashingRelation till it hit the memory limits</li>
       *   <li>Determine whether the hashingRelation is fully consumed</li>
       * </ol>
       */
      private void doHashing() {
        this.keyHash.clear();
        // building the hash, as long as memory permits
        while (Runtime.getRuntime().freeMemory() < amountToKeepFree && hashingRelation.hasNext()) {
          Tuple<?, ?> nextLeft = hashingRelation.next();
          keyHash.computeIfAbsent(nextLeft.getKey(), k -> new ArrayList()).add(nextLeft.getValue());
        }

        // determine whether hashRelation is fully consumed
        hashingDone = !hashingRelation.hasNext();

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

      // when iterating over the right(probing) relation, current element
      // (which has been returned by next()) will be kept in memory since it should be combined
      // with all the tuples in leftListForCurrentKey. But this has to be done on demand, on next()
      // call of joined iterator.
      private Tuple<?, ?> currentProbingTuple;

      // list of tuples from left relation(hashing relation),
      // that matches with the currentRightTuple
      private List leftListForCurrentKey;

      // keeping the index of leftListForCurrentKey
      private int leftListIndex = 0;

      /**
       * This method should be guaranteed to create a {@link JoinedTuple}. If a tuple can't be
       * created, caller should determine that before calling this method.
       * Additionally, this method should clear everything if everything related to
       * currentRightTuple is processed.
       */
      private void progressProbing() {
        Object key = this.currentProbingTuple.getKey();

        // we have interchanged original iterators based on the join type.
        // that should be taken into consideration when creating the JoinedTuple
        Object left = joinType.equals(CommunicationContext.JoinType.LEFT)
            ? this.currentProbingTuple.getValue() : leftListForCurrentKey.get(leftListIndex);
        Object right = joinType.equals(CommunicationContext.JoinType.LEFT)
            ? leftListForCurrentKey.get(leftListIndex) : this.currentProbingTuple.getValue();

        this.nextJoinTuple = JoinedTuple.of(
            key,
            left,
            right
        );

        leftListIndex++;

        // if end of the list has reached, reset everything!
        if (leftListIndex == leftListForCurrentKey.size()) {
          currentProbingTuple = null;
          leftListForCurrentKey = null;
          leftListIndex = 0;
        }
      }

      /**
       * This method iterates through the right relation(probing relation).
       */
      private void doProbing() {
        // if there is a non null nextJoinTuple, no need of proceeding
        while (this.nextJoinTuple == null) {
          // if the currentRightTuple is non null, that means we have already found the relevant
          // hashed list and still in the middle of combining that list
          if (this.currentProbingTuple == null) {
            if (probingRelation.hasNext()) {
              this.currentProbingTuple = probingRelation.next();
              this.leftListForCurrentKey = this.keyHash.get(currentProbingTuple.getKey());
              if (this.leftListForCurrentKey == null) {
                // not left tuples from hashing relation to join

                // handle left and right joins here
                if (joinType.equals(CommunicationContext.JoinType.LEFT)) {
                  this.nextJoinTuple = JoinedTuple.of(
                      currentProbingTuple.getKey(),
                      currentProbingTuple.getValue(),
                      null
                  );
                } else if (joinType.equals(CommunicationContext.JoinType.RIGHT)) {
                  this.nextJoinTuple = JoinedTuple.of(
                      currentProbingTuple.getKey(),
                      null,
                      currentProbingTuple.getValue()
                  );
                }

                // any join : We are done with currentProbingTuple
                this.currentProbingTuple = null;
              } else {
                progressProbing();
              }
            } else {
              // right iterator has reached to an end for current HashMap.
              if (!hashingDone) {
                // clear current hash and reset the right iterator
                doHashing();
                probingRelation.reset();
              } else {
                // end of join operation. Yay!
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
        nextJoinTuple = null;

        // create the next JoinTuple before returning
        doProbing();
        return currentJoinTuple;
      }
    };
  }
}
