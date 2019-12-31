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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.ResettableIterator;

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

    int noOfTuples = 1000;

    List<Integer> keys1 = new ArrayList<>();
    List<Integer> keys2 = new ArrayList<>();
    for (int i = 0; i < noOfTuples; i++) {
      keys1.add(i);
      keys2.add(i);
    }
    Collections.shuffle(keys1);
    Collections.shuffle(keys2);

    FSKeyedMerger fsMerger1 = new FSKeyedMerger(0, 0,
        "/tmp", "op-left", MessageTypes.INTEGER, MessageTypes.INTEGER);

    FSKeyedMerger fsMerger2 = new FSKeyedMerger(0, 0,
        "/tmp", "op-right", MessageTypes.INTEGER, MessageTypes.INTEGER);

    byte[] key1 = ByteBuffer.wrap(new byte[4]).putInt(1).array();
    byte[] key2 = ByteBuffer.wrap(new byte[4]).putInt(2).array();

    for (int i = 0; i < noOfTuples; i++) {
      fsMerger1.add(keys1.get(i), key1, Integer.BYTES);
      fsMerger2.add(keys2.get(i), key2, Integer.BYTES);
      fsMerger1.run();
      fsMerger2.run();
    }

    fsMerger1.switchToReading();
    fsMerger2.switchToReading();

    ResettableIterator it1 = fsMerger1.readIterator();
    ResettableIterator it2 = fsMerger2.readIterator();

    Iterator<JoinedTuple> iterator = HashJoinUtils.innerJoin(it1, it2,
        CommunicationContext.JoinType.INNER);

    Set<Integer> keysReceived = new HashSet<>();

    while (iterator.hasNext()) {
      JoinedTuple joinedTuple = iterator.next();
      Assert.assertEquals(1, joinedTuple.getLeftValue());
      Assert.assertEquals(2, joinedTuple.getRightValue());
      keysReceived.add((Integer) joinedTuple.getKey());
    }

    Assert.assertEquals(noOfTuples, keysReceived.size());

    fsMerger1.clean();
    fsMerger2.clean();
  }
}
