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
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.shuffle.RestorableIterator;

public final class SortJoinUtils {

  private static final Logger LOG = Logger.getLogger(SortJoinUtils.class.getName());

  public static final String CONFIG_USE_SORT_JOIN_CACHE = "twister2.join.sort.cache";

  private SortJoinUtils() {
  }

  public static Iterator<JoinedTuple> join(List<Tuple> leftRelation,
                                           List<Tuple> rightRelation,
                                           KeyComparatorWrapper comparator,
                                           CommunicationContext.JoinType joinType) {
    RestorableIterator leftRstIt = new ListBasedRestorableIterator(leftRelation);
    RestorableIterator rightRstIt = new ListBasedRestorableIterator(rightRelation);
    if (joinType == CommunicationContext.JoinType.INNER) {
      return innerJoin(leftRstIt, rightRstIt, comparator);
    } else {
      return outerJoin(leftRstIt, rightRstIt, comparator, joinType);
    }
  }

  public static Iterator<JoinedTuple> join(RestorableIterator<Tuple<?, ?>> leftIt,
                                           RestorableIterator<Tuple<?, ?>> rightIt,
                                           KeyComparatorWrapper comparator,
                                           CommunicationContext.JoinType joinType) {
    if (joinType == CommunicationContext.JoinType.INNER) {
      return innerJoin(leftIt, rightIt, comparator);
    } else {
      return outerJoin(leftIt, rightIt, comparator, joinType);
    }
  }

  /**
   * Inner join the left and right relation using the tuple key
   *
   * @param leftRelation left relation
   * @param rightRelation right relation
   * @param comparator comparator
   * @return the joined relation
   */
  public static List<Object> innerJoin(List<Tuple> leftRelation,
                                       List<Tuple> rightRelation,
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
        outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), right.getValue()));

        int index = leftIndex + 1;
        while (index < leftRelation.size()) {
          Tuple l = leftRelation.get(index);

          if (comparator.compare(l, right) == 0) {
            outPut.add(new JoinedTuple<>(l.getKey(), l.getValue(), right.getValue()));
          } else {
            break;
          }
          index++;
        }

        index = rightIndex + 1;
        while (index < rightRelation.size()) {
          Tuple r = rightRelation.get(index);
          if (comparator.compare(left, r) == 0) {
            outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), r.getValue()));
          } else {
            break;
          }
          index++;
        }
        leftIndex++;
        rightIndex++;
      } else if (comparator.compare(left, right) < 0) {
        leftIndex++;
      } else {
        rightIndex++;
      }
    }

    return outPut;
  }

  /**
   * This method avoid having to scan back and forth of the files by reading data iterators once
   * and backup them into a {@link DiskBasedList}, which has a memory buffer
   */
  public static Iterator<JoinedTuple> joinWithCache(RestorableIterator<Tuple<?, ?>> leftIt,
                                                    RestorableIterator<Tuple<?, ?>> rightIt,
                                                    KeyComparatorWrapper comparator,
                                                    CommunicationContext.JoinType joinType,
                                                    Config config) {
    LOG.info("Performing join with cache....");
    return new Iterator<JoinedTuple>() {

      private final List<DiskBasedList> oldLists = new ArrayList<>();

      private DiskBasedList leftList;
      private DiskBasedList rightList;

      // if we had to keep next() to check the next tuple, these variables can be used to keep them
      private Tuple leftBackup;
      private Tuple rightBackup;

      private Iterator<JoinedTuple> localJoinIterator;

      /**
       * Advances two iterators by reading onto memory
       *
       * @return true if advance() should be called again
       */
      private boolean advance() {
        if (this.leftList != null) {
          this.leftList.clear();
          //this.oldLists.add(this.leftList);
        }

        if (this.rightList != null) {
          this.rightList.clear();
          //this.oldLists.add(this.rightList);
        }

        long maxRecordsInMemory = CommunicationContext.getShuffleMaxRecordsInMemory(config) / 2;

        // previous lists are now garbage collectible
        this.leftList = new DiskBasedList(config, MessageTypes.OBJECT);
        this.rightList = new DiskBasedList(config, MessageTypes.OBJECT);

        Tuple currentTuple = null;

        // read from left iterator
        while (leftIt.hasNext() || this.leftBackup != null) {
          Tuple<?, ?> nextLeft = this.leftBackup != null ? this.leftBackup : leftIt.next();
          this.leftBackup = null; // we used the backup
          if (currentTuple == null) {
            currentTuple = nextLeft;
          }
          if (comparator.compare(currentTuple, nextLeft) == 0) {
            this.leftList.add(nextLeft);
          } else if (comparator.compare(currentTuple, nextLeft) < 0
              && this.leftList.size() < maxRecordsInMemory) {
            currentTuple = nextLeft;
            this.leftList.add(nextLeft);
          } else {
            this.leftBackup = nextLeft;
            break;
          }
        }

        // read from right iterator
        while (rightIt.hasNext() || this.rightBackup != null) {
          Tuple<?, ?> nextRight = this.rightBackup != null ? this.rightBackup : rightIt.next();
          this.rightBackup = null;
          if (currentTuple == null) {
            currentTuple = nextRight;
          }
          if (comparator.compare(currentTuple, nextRight) >= 0) {
            this.rightList.add(nextRight);
          } else {
            this.rightBackup = nextRight;
            break;
          }
        }

        this.localJoinIterator = join(
            new ListBasedRestorableIterator(this.leftList),
            new ListBasedRestorableIterator(this.rightList),
            comparator,
            joinType
        );

        // if current local iterators doesn't has join tuples and if we have more data on
        // data iterators, let's advance() again
        return !this.localJoinIterator.hasNext()
            && (leftBackup != null || rightBackup != null
            || leftIt.hasNext() || rightIt.hasNext());
      }

      private void callAdvanceIt() {
        boolean shouldCall = true;
        while (shouldCall) {
          shouldCall = this.advance();
        }
      }

      {
        this.callAdvanceIt();

        // add a shutdown hook to cleanup
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public synchronized void start() {
            LOG.info("Cleaning up disk based caches used for join...");
            for (DiskBasedList oldList : oldLists) {
              oldList.clear();
            }
          }
        });
      }

      @Override
      public boolean hasNext() {
        return this.localJoinIterator != null && this.localJoinIterator.hasNext();
      }

      @Override
      public JoinedTuple next() {
        JoinedTuple next = this.localJoinIterator.next();
        if (!this.localJoinIterator.hasNext()) {
          this.callAdvanceIt();
        }
        return next;
      }
    };
  }

  /**
   * This util can be used to perform disk based inner join operations.
   */
  public static Iterator<JoinedTuple> innerJoin(RestorableIterator<Tuple<?, ?>> leftIt,
                                                RestorableIterator<Tuple<?, ?>> rightIt,
                                                KeyComparatorWrapper comparator) {
    return new Iterator<JoinedTuple>() {

      private JoinedTuple nextJoinTuple;

      private Tuple currentLeft;
      private Tuple currentRight;

      // backup variables will hold a Tuple temporary if had to call .next()
      // once during the join operation before creating a iterator restore point.
      private Tuple backedUpLeft;
      private Tuple backedUpRight;

      // flags to mark the required side of iteration
      private boolean shouldDoLeftIterations = false;
      private boolean shouldDoRightIterations = false;

      // keeps the no of iterations done on each side of the relationship while keeping the
      // other side constant
      private int leftIterations = 0;
      private int rightIterations = 0;

      private JoinedTuple doLeftIteration() {
        if (!shouldDoLeftIterations) {
          return null;
        }
        JoinedTuple jtFromLeftIt = null;
        if (leftIt.hasNext()) {
          Tuple l = leftIt.next();
          if (this.leftIterations == 0) {
            this.backedUpLeft = l;
          }
          if (comparator.compare(l, this.currentRight) == 0) {
            if (this.leftIterations == 0) {
              leftIt.createRestorePoint();
            }
            this.leftIterations++;
            jtFromLeftIt = new JoinedTuple<>(l.getKey(), l.getValue(),
                this.currentRight.getValue());
          }
        }

        /*
         if this is the end of left iteration(jtFromLeftIt == null), configure the right iterations
         to run next and restore left iterator
        */
        if (jtFromLeftIt == null) {
          this.leftIterations = 0;
          this.shouldDoLeftIterations = false;
          this.shouldDoRightIterations = true;
          if (leftIt.hasRestorePoint()) {
            leftIt.restore();
            leftIt.clearRestorePoint();
          }
        }
        return jtFromLeftIt;
      }

      private JoinedTuple doRightIteration() {
        if (!shouldDoRightIterations) {
          return null;
        }
        JoinedTuple jtFromRightIt = null;
        if (rightIt.hasNext()) {
          Tuple l = rightIt.next();
          if (this.rightIterations == 0) {
            this.backedUpRight = l;
          }
          if (comparator.compare(this.currentLeft, l) == 0) {
            if (this.rightIterations == 0) {
              rightIt.createRestorePoint();
            }
            this.rightIterations++;
            jtFromRightIt = new JoinedTuple<>(l.getKey(), this.currentLeft.getValue(),
                l.getValue());
          }
        }

        /*
         if this is the end of left iteration(jtFromRightIt == null), configure the right iterations
         to run next and restore left iterator
        */
        if (jtFromRightIt == null) {
          this.rightIterations = 0;
          this.shouldDoRightIterations = false;
          if (rightIt.hasRestorePoint()) {
            rightIt.restore();
            rightIt.clearRestorePoint();
          }
        }
        return jtFromRightIt;
      }

      private void makeNextJoinTuple() {
        nextJoinTuple = this.doLeftIteration();
        if (nextJoinTuple == null) {
          nextJoinTuple = this.doRightIteration();
        }
        while (nextJoinTuple == null
            && (this.backedUpLeft != null || leftIt.hasNext())
            && (this.backedUpRight != null || rightIt.hasNext())) {
          this.currentLeft = this.backedUpLeft != null ? this.backedUpLeft : leftIt.next();
          this.backedUpLeft = null; // we used the backup, so setting to null

          this.currentRight = this.backedUpRight != null ? this.backedUpRight : rightIt.next();
          this.backedUpRight = null;

          // still we don't need left or right iterations at this point
          this.shouldDoLeftIterations = false;
          this.shouldDoRightIterations = false;

          if (comparator.compare(this.currentLeft, this.currentRight) == 0) {
            this.nextJoinTuple = new JoinedTuple<>(this.currentLeft.getKey(),
                this.currentLeft.getValue(), this.currentRight.getValue());
            // schedule to run the left iteration next.
            // Left iteration at the end will schedule right iteration
            this.shouldDoLeftIterations = true;
            break;
          } else if (comparator.compare(this.currentLeft, this.currentRight) < 0) {
            if (leftIt.hasNext()) {
              this.backedUpLeft = leftIt.next();
            }
            this.backedUpRight = this.currentRight;
          } else {
            if (rightIt.hasNext()) {
              this.backedUpRight = rightIt.next();
            }
            this.backedUpLeft = this.currentLeft;
          }
        }
      }

      {
        // start by creating the first join tuple
        this.makeNextJoinTuple();
      }

      @Override
      public boolean hasNext() {
        return nextJoinTuple != null;
      }

      @Override
      public JoinedTuple next() {
        JoinedTuple current = nextJoinTuple;
        this.makeNextJoinTuple();
        return current;
      }
    };
  }

  /**
   * This util can be used to perform disk based inner join operations.
   */
  public static Iterator<JoinedTuple> outerJoin(RestorableIterator<Tuple<?, ?>> leftIt,
                                                RestorableIterator<Tuple<?, ?>> rightIt,
                                                KeyComparatorWrapper comparator,
                                                CommunicationContext.JoinType outerJoinType) {
    return new Iterator<JoinedTuple>() {

      private JoinedTuple nextJoinTuple;

      private Tuple currentLeft;
      private Tuple currentRight;

      // backup variables will hold a Tuple temporary if had to call .next()
      // once during the join operation before creating a iterator restore point.
      private Tuple backedUpLeft;
      private Tuple backedUpRight;

      // flags to mark the required side of iteration
      private boolean shouldDoLeftIterations = false;
      private boolean shouldDoRightIterations = false;

      private JoinedTuple doLeftIteration() {
        if (!shouldDoLeftIterations) {
          return null;
        }
        JoinedTuple jtFromLeftIt = null;
        if (leftIt.hasNext()) {
          Tuple l = leftIt.next();
          if (comparator.compare(l, this.currentRight) == 0) {
            jtFromLeftIt = new JoinedTuple<>(l.getKey(), l.getValue(),
                this.currentRight.getValue());
          } else {
            this.backedUpLeft = l;
          }
        }

        /*
         if this is the end of left iteration(jtFromLeftIt == null), configure the right iterations
         to run next and restore left iterator
        */
        if (jtFromLeftIt == null) {
          this.shouldDoLeftIterations = false;
          this.shouldDoRightIterations = true;
        }
        return jtFromLeftIt;
      }

      private JoinedTuple doRightIteration() {
        if (!shouldDoRightIterations) {
          return null;
        }
        JoinedTuple jtFromRightIt = null;
        if (rightIt.hasNext()) {
          Tuple l = rightIt.next();
          if (comparator.compare(this.currentLeft, l) == 0) {
            jtFromRightIt = new JoinedTuple<>(l.getKey(), this.currentLeft.getValue(),
                l.getValue());
          } else {
            this.backedUpRight = l;
          }
        }

        /*
         if this is the end of left iteration(jtFromRightIt == null), configure the right iterations
         to run next and restore left iterator
        */
        if (jtFromRightIt == null) {
          this.shouldDoRightIterations = false;
        }
        return jtFromRightIt;
      }

      private void makeNextJoinTuple() {
        nextJoinTuple = this.doLeftIteration();
        if (nextJoinTuple == null) {
          nextJoinTuple = this.doRightIteration();
        }
        while (nextJoinTuple == null
            && (this.backedUpLeft != null || leftIt.hasNext())
            && (this.backedUpRight != null || rightIt.hasNext())) {
          this.currentLeft = this.backedUpLeft != null ? this.backedUpLeft : leftIt.next();
          this.backedUpLeft = null; // we used the backup, so setting to null

          this.currentRight = this.backedUpRight != null ? this.backedUpRight : rightIt.next();
          this.backedUpRight = null;

          // still we don't need left or right iterations at this point
          this.shouldDoLeftIterations = false;
          this.shouldDoRightIterations = false;

          if (comparator.compare(this.currentLeft, this.currentRight) == 0) {
            this.nextJoinTuple = new JoinedTuple<>(this.currentLeft.getKey(),
                this.currentLeft.getValue(), this.currentRight.getValue());
            // schedule to run the left iteration next.
            // Left iteration at the end will schedule right iteration
            this.shouldDoLeftIterations = true;
            break;
          } else if (comparator.compare(this.currentLeft, this.currentRight) < 0) {
            if (outerJoinType.includeLeft()) {
              this.nextJoinTuple = new JoinedTuple<>(this.currentLeft.getKey(),
                  this.currentLeft.getValue(), null);
            }

            if (leftIt.hasNext()) {
              this.backedUpLeft = leftIt.next();
            }
            this.backedUpRight = this.currentRight;
          } else {
            if (outerJoinType.includeRight()) {
              this.nextJoinTuple = new JoinedTuple<>(this.currentRight.getKey(),
                  null, this.currentRight.getValue());
            }

            if (rightIt.hasNext()) {
              this.backedUpRight = rightIt.next();
            }
            this.backedUpLeft = this.currentLeft;
          }
        }
      }

      {
        // start by creating the first join tuple
        this.makeNextJoinTuple();
      }

      @Override
      public boolean hasNext() {
        return nextJoinTuple != null;
      }

      @Override
      public JoinedTuple next() {
        JoinedTuple current = nextJoinTuple;
        this.makeNextJoinTuple();
        return current;
      }
    };
  }

  /**
   * Full Outer join the left and right relation using the tuple key
   */
  private static List<Object> outerJoin(List<Tuple> leftRelation,
                                        List<Tuple> rightRelation,
                                        KeyComparatorWrapper comparator,
                                        CommunicationContext.JoinType outerJoinType) {
    int leftIndex = 0;
    int rightIndex = 0;

    leftRelation.sort(comparator);
    rightRelation.sort(comparator);

    List<Object> outPut = new ArrayList<>();
    while (leftIndex < leftRelation.size() && rightIndex < rightRelation.size()) {
      Tuple left = leftRelation.get(leftIndex);
      Tuple right = rightRelation.get(rightIndex);

      if (comparator.compare(left, right) == 0) {
        outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), right.getValue()));

        int index = leftIndex + 1;
        while (index < leftRelation.size()) {
          Tuple l = leftRelation.get(index);

          if (comparator.compare(l, right) == 0) {
            outPut.add(new JoinedTuple<>(l.getKey(), l.getValue(), right.getValue()));
          } else {
            break;
          }
          index++;
        }

        leftIndex = index;

        index = rightIndex + 1;
        while (index < rightRelation.size()) {
          Tuple r = rightRelation.get(index);
          if (comparator.compare(left, r) == 0) {
            outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), r.getValue()));
          } else {
            break;
          }
          index++;
        }

        rightIndex = index;
      } else if (comparator.compare(left, right) < 0) {
        if (outerJoinType.includeLeft()) {
          outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), null));
        }
        leftIndex++;
      } else {
        if (outerJoinType.includeRight()) {
          outPut.add(new JoinedTuple<>(right.getKey(), null, right.getValue()));
        }
        rightIndex++;
      }
    }

    while (leftIndex < leftRelation.size() && outerJoinType.includeLeft()) {
      Tuple left = leftRelation.get(leftIndex);
      outPut.add(new JoinedTuple<>(left.getKey(), left.getValue(), null));
      leftIndex++;
    }

    while (rightIndex < rightRelation.size() && outerJoinType.includeRight()) {
      Tuple right = rightRelation.get(rightIndex);
      outPut.add(new JoinedTuple<>(right.getKey(), null, right.getValue()));
      rightIndex++;
    }

    return outPut;
  }

  /**
   * Full Outer join the left and right relation using the tuple key
   */
  public static Iterator<JoinedTuple> fullOuterJoin(RestorableIterator<Tuple<?, ?>> leftIt,
                                                    RestorableIterator<Tuple<?, ?>> rightIt,
                                                    KeyComparatorWrapper comparator) {
    return outerJoin(leftIt, rightIt, comparator, CommunicationContext.JoinType.FULL_OUTER);
  }

  /**
   * Left Outer join the left and right relation using the tuple key
   */
  public static Iterator<JoinedTuple> leftOuterJoin(RestorableIterator<Tuple<?, ?>> leftIt,
                                                    RestorableIterator<Tuple<?, ?>> rightIt,
                                                    KeyComparatorWrapper comparator) {
    return outerJoin(leftIt, rightIt, comparator, CommunicationContext.JoinType.LEFT);
  }

  /**
   * Right Outer join the left and right relation using the tuple key
   */
  public static Iterator<JoinedTuple> rightOuterJoin(RestorableIterator<Tuple<?, ?>> leftIt,
                                                     RestorableIterator<Tuple<?, ?>> rightIt,
                                                     KeyComparatorWrapper comparator) {
    return outerJoin(leftIt, rightIt, comparator, CommunicationContext.JoinType.RIGHT);
  }

  /**
   * Full Outer join the left and right relation using the tuple key
   */
  public static List<Object> fullOuterJoin(List<Tuple> leftRelation,
                                           List<Tuple> rightRelation,
                                           KeyComparatorWrapper comparator) {
    return outerJoin(leftRelation, rightRelation, comparator,
        CommunicationContext.JoinType.FULL_OUTER);
  }

  /**
   * Left Outer join the left and right relation using the tuple key
   */
  public static List<Object> leftOuterJoin(List<Tuple> leftRelation,
                                           List<Tuple> rightRelation,
                                           KeyComparatorWrapper comparator) {
    return outerJoin(leftRelation, rightRelation, comparator, CommunicationContext.JoinType.LEFT);
  }

  /**
   * Right Outer join the left and right relation using the tuple key
   */
  public static List<Object> rightOuterJoin(List<Tuple> leftRelation,
                                            List<Tuple> rightRelation,
                                            KeyComparatorWrapper comparator) {
    return outerJoin(leftRelation, rightRelation, comparator, CommunicationContext.JoinType.RIGHT);
  }


  public static class ListBasedRestorableIterator implements RestorableIterator {

    private List list;
    private int currentIndex = 0;
    private int backedUpIndex = -1;

    ListBasedRestorableIterator(List list) {
      this.list = list;
    }

    @Override
    public void createRestorePoint() {
      this.backedUpIndex = this.currentIndex;
    }

    @Override
    public void restore() {
      this.currentIndex = this.backedUpIndex;
    }

    @Override
    public boolean hasRestorePoint() {
      return this.backedUpIndex != -1;
    }

    @Override
    public void clearRestorePoint() {
      this.backedUpIndex = -1;
    }

    @Override
    public boolean hasNext() {
      return this.currentIndex < this.list.size();
    }

    @Override
    public Object next() {
      return this.list.get(this.currentIndex++);
    }
  }
}
