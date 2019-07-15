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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.util.CommonThreadPool;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger2;
import edu.iu.dsc.tws.comms.shuffle.RestorableIterator;

public class JoinUtilsTest {

  private static final Logger LOG = Logger.getLogger(JoinUtilsTest.class.getName());

  /**
   * Example values from https://en.wikipedia.org/wiki/Join_(SQL)
   */
  private List<Tuple> getEmployees() {
    List<Tuple> employees = new ArrayList<>();

    employees.add(Tuple.of(31, "Rafferty"));
    employees.add(Tuple.of(33, "Jones"));
    employees.add(Tuple.of(33, "Heisenberg"));
    employees.add(Tuple.of(34, "Robinson"));
    employees.add(Tuple.of(34, "Smith"));
    employees.add(Tuple.of(null, "Williams"));

    return employees;
  }

  private List<Tuple> getDepartments() {
    List<Tuple> departments = new ArrayList<>();

    departments.add(Tuple.of(31, "Sales"));
    departments.add(Tuple.of(33, "Engineering"));
    departments.add(Tuple.of(34, "Clerical"));
    departments.add(Tuple.of(35, "Marketing"));

    return departments;
  }

  private List<Object> getInnerJoined() {
    List<Object> innerJoined = new ArrayList<>();
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    return innerJoined;
  }

  private List<Object> getFullOuterJoined() {
    List<Object> innerJoined = new ArrayList<>();
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(null, "Williams", null));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    innerJoined.add(new JoinedTuple(35, null, "Marketing"));
    return innerJoined;
  }

  private List<Object> getLeftOuterJoined() {
    List<Object> innerJoined = new ArrayList<>();
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(null, "Williams", null));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    return innerJoined;
  }

  private List<Object> getRightOuterJoined() {
    List<Object> innerJoined = new ArrayList<>();

    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    innerJoined.add(new JoinedTuple(35, null, "Marketing"));
    return innerJoined;
  }

  private KeyComparatorWrapper getEmployeeDepComparator() {
    return new KeyComparatorWrapper((Comparator<Integer>) (o1, o2) -> {
      if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    });
  }

  private Comparator<Object> getJoinedTupleComparator() {
    return (o1, o2) -> {
      Integer k1 = (Integer) ((JoinedTuple) o1).getKey();
      Integer k2 = (Integer) ((JoinedTuple) o2).getKey();
      if (k1 == null) {
        return -1;
      } else if (k2 == null) {
        return 1;
      }
      return k1.compareTo(k2);
    };
  }

  /**
   * This test compares the results of in memory and disk based inner joins.
   * Purpose is to verify the accuracy of disk based inner join
   */
  @Test
  public void innerJoinComparision() {
    List<Tuple> left = new ArrayList<>();
    List<Tuple> right = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
      left.add(Tuple.of(random.nextInt(10), random.nextInt()));
      right.add(Tuple.of(random.nextInt(10), random.nextInt()));
    }

    FSKeyedSortedMerger2 fsk1 = new FSKeyedSortedMerger2(
        10,
        100,
        "/tmp",
        "op-1-" + UUID.randomUUID().toString(),
        MessageTypes.INTEGER,
        MessageTypes.INTEGER,
        (Comparator<Integer>) Integer::compare,
        0,
        false,
        1
    );

    for (Tuple tuple : left) {
      byte[] data = MessageTypes.INTEGER.getDataPacker()
          .packToByteArray((Integer) tuple.getValue());
      fsk1.add(tuple.getKey(), data, data.length);
      fsk1.run();
    }

    FSKeyedSortedMerger2 fsk2 = new FSKeyedSortedMerger2(
        10,
        100,
        "/tmp",
        "op-2-" + UUID.randomUUID().toString(),
        MessageTypes.INTEGER,
        MessageTypes.INTEGER,
        (Comparator<Integer>) Integer::compare,
        0,
        false,
        1
    );

    for (Tuple tuple : right) {
      byte[] data = MessageTypes.INTEGER.getDataPacker()
          .packToByteArray((Integer) tuple.getValue());
      fsk2.add(tuple.getKey(), data, data.length);
      fsk2.run();
    }

    CommonThreadPool.init(Config.newBuilder().build());

    fsk1.switchToReading();
    fsk2.switchToReading();

    Iterator iterator = JoinUtils.innerJoin(
        (RestorableIterator) fsk1.readIterator(),
        (RestorableIterator) fsk2.readIterator(),
        new KeyComparatorWrapper((Comparator<Integer>) Integer::compare)
    );


    List<Object> objects = JoinUtils.innerJoin(left, right,
        new KeyComparatorWrapper(Comparator.naturalOrder()));

    objects.sort(Comparator.comparingInt(o -> (Integer) ((JoinedTuple) o).getKey()));


    int i = 0;
    while (iterator.hasNext()) {
      JoinedTuple nextFromIt = (JoinedTuple) iterator.next();
      JoinedTuple nextFromList = (JoinedTuple) objects.get(i++);

      Assert.assertEquals(nextFromIt.getKey(), nextFromList.getKey());
    }


    Assert.assertEquals(i, objects.size());

    LOG.info(objects.toString());
  }

  /**
   * This test compares the in memory inner join against a known set of results.
   * Purpose is to verify the accuracy of in memory inner join
   */
  @Test
  public void innerJoinInMemory() {
    List<Tuple> employees = this.getEmployees();
    List<Tuple> departments = this.getDepartments();
    List<Object> joined = JoinUtils.innerJoin(
        employees,
        departments,
        this.getEmployeeDepComparator()
    );

    List<Object> innerJoined = this.getInnerJoined();

    joined.sort(this.getJoinedTupleComparator());
    innerJoined.sort(this.getJoinedTupleComparator());

    Assert.assertEquals(joined.size(), innerJoined.size());

    for (int i = 0; i < innerJoined.size(); i++) {
      Assert.assertEquals(innerJoined.get(i), joined.get(i));
    }
  }

  /**
   * This test compares the in memory full outer join against a known set of results.
   * Purpose is to verify the accuracy of in memory full outer join
   */
  @Test
  public void fullOuterJoinInMemory() {
    List<Tuple> employees = this.getEmployees();
    List<Tuple> departments = this.getDepartments();
    List<Object> joined = JoinUtils.fullOuterJoin(
        employees,
        departments,
        this.getEmployeeDepComparator()
    );

    List<Object> outerJoined = this.getFullOuterJoined();

    joined.sort(this.getJoinedTupleComparator());
    outerJoined.sort(this.getJoinedTupleComparator());

    Assert.assertEquals(joined.size(), outerJoined.size());

    for (int i = 0; i < outerJoined.size(); i++) {
      Assert.assertEquals(outerJoined.get(i), joined.get(i));
    }
  }

  /**
   * This test compares the in memory left outer join against a known set of results.
   * Purpose is to verify the accuracy of in memory left outer join
   */
  @Test
  public void leftOuterJoinInMemory() {
    List<Tuple> employees = this.getEmployees();
    List<Tuple> departments = this.getDepartments();
    List<Object> joined = JoinUtils.leftOuterJoin(
        employees,
        departments,
        this.getEmployeeDepComparator()
    );

    List<Object> leftOuterJoined = this.getLeftOuterJoined();

    joined.sort(this.getJoinedTupleComparator());
    leftOuterJoined.sort(this.getJoinedTupleComparator());

    Assert.assertEquals(joined.size(), leftOuterJoined.size());

    for (int i = 0; i < leftOuterJoined.size(); i++) {
      Assert.assertEquals(leftOuterJoined.get(i), joined.get(i));
    }
  }

  /**
   * This test compares the in memory right outer join against a known set of results.
   * Purpose is to verify the accuracy of in memory left outer join
   */
  @Test
  public void rightOuterJoinInMemory() {
    List<Tuple> employees = this.getEmployees();
    List<Tuple> departments = this.getDepartments();
    List<Object> joined = JoinUtils.rightOuterJoin(
        employees,
        departments,
        this.getEmployeeDepComparator()
    );

    List<Object> rightOuterJoined = this.getRightOuterJoined();

    joined.sort(this.getJoinedTupleComparator());
    rightOuterJoined.sort(this.getJoinedTupleComparator());

    Assert.assertEquals(joined.size(), rightOuterJoined.size());

    for (int i = 0; i < rightOuterJoined.size(); i++) {
      Assert.assertEquals(rightOuterJoined.get(i), joined.get(i));
    }
  }
}
