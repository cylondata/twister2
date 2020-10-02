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
import java.util.List;

import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;

public final class JoinTestUtils {

  private JoinTestUtils() {
  }

  /**
   * Example values from https://en.wikipedia.org/wiki/Join_(SQL)
   */
  public static List<Tuple> getEmployees() {
    List<Tuple> employees = new ArrayList<>();

    employees.add(Tuple.of(31, "Rafferty"));
    employees.add(Tuple.of(33, "Jones"));
    employees.add(Tuple.of(33, "Heisenberg"));
    employees.add(Tuple.of(34, "Robinson"));
    employees.add(Tuple.of(34, "Smith"));
    employees.add(Tuple.of(null, "Williams"));

    return employees;
  }

  public static List<Tuple> getDepartments() {
    List<Tuple> departments = new ArrayList<>();

    departments.add(Tuple.of(31, "Sales"));
    departments.add(Tuple.of(33, "Engineering"));
    departments.add(Tuple.of(34, "Clerical"));
    departments.add(Tuple.of(35, "Marketing"));

    return departments;
  }

  public static List<Object> getInnerJoined() {
    List<Object> innerJoined = new ArrayList<>();
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    return innerJoined;
  }

  public static List<Object> getFullOuterJoined() {
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

  public static List<Object> getLeftOuterJoined() {
    List<Object> innerJoined = new ArrayList<>();
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(null, "Williams", null));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    return innerJoined;
  }

  public static List<Object> getRightOuterJoined() {
    List<Object> innerJoined = new ArrayList<>();

    innerJoined.add(new JoinedTuple(34, "Smith", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Jones", "Engineering"));
    innerJoined.add(new JoinedTuple(34, "Robinson", "Clerical"));
    innerJoined.add(new JoinedTuple(33, "Heisenberg", "Engineering"));
    innerJoined.add(new JoinedTuple(31, "Rafferty", "Sales"));
    innerJoined.add(new JoinedTuple(35, null, "Marketing"));
    return innerJoined;
  }

  public static KeyComparatorWrapper getEmployeeDepComparator() {
    return new KeyComparatorWrapper((Comparator<Integer>) (o1, o2) -> {
      if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      return o1.compareTo(o2);
    });
  }

  public static Comparator<Object> getJoinedTupleComparator() {
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
}
