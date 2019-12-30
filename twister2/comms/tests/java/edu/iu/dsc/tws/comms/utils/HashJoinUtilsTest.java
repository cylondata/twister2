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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSMerger;

public class HashJoinUtilsTest {

  @Test
  public void innerJoinMemoryTest() {
    List<Tuple> departments = JoinTestUtils.getDepartments();
    List<Tuple> employees = JoinTestUtils.getEmployees();

    KeyComparatorWrapper employeeDepComparator = JoinTestUtils.getEmployeeDepComparator();

    List<Object> joined = HashJoinUtils.innerJoin(
        employees,
        departments,
        employeeDepComparator
    );

    List<Object> innerJoined = JoinTestUtils.getInnerJoined();

    Assert.assertEquals(innerJoined.size(), joined.size());

    joined.sort(JoinTestUtils.getJoinedTupleComparator());
    innerJoined.sort(JoinTestUtils.getJoinedTupleComparator());

    for (int i = 0; i < innerJoined.size(); i++) {
      Assert.assertEquals(innerJoined.get(i), joined.get(i));
    }
  }

  @Test
  public void innerJoinMemoryVsDiskTest() {
    FSKeyedMerger fsMerger1 = new FSKeyedMerger(0, 0,
        "/tmp", "op", MessageTypes.INTEGER, MessageTypes.INTEGER);

    List<Integer> keys = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {

    }

    fsMerger1.switchToReading();
  }
}
