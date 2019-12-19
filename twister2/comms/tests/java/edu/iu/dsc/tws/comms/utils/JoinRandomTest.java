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
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.structs.Tuple;

/**
 * This class will generate some random data and apply multiple join implementations
 * and cross check the results
 */
public class JoinRandomTest {

  private static final Logger LOG = Logger.getLogger(JoinTestUtils.class.getName());
  private static final int ARRAY_SIZE = 1000000;

  private Random random = new Random(System.currentTimeMillis());

  private List<Tuple> randomData() {
    List<Tuple> tuples = new ArrayList<>();

    for (int i = 0; i < JoinRandomTest.ARRAY_SIZE * random.nextInt(100); i++) {
      tuples.add(Tuple.of(random.nextInt(), i));
    }
    return tuples;
  }

  private KeyComparatorWrapper intComparator() {
    return new KeyComparatorWrapper(Comparator.comparingInt(o -> (int) o));
  }

  @Test
  public void innerJoinTest() {
    List<Tuple> leftRelation = randomData();
    List<Tuple> rightRelation = randomData();

    KeyComparatorWrapper intComparator = intComparator();

    long t1 = System.currentTimeMillis();
    List<Object> hashResult = HashJoinUtils.innerJoin(leftRelation, rightRelation, intComparator);
    LOG.info("Time for hash join : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    List<Object> sortResult = SortJoinUtils.innerJoin(leftRelation, rightRelation, intComparator);
    LOG.info("Time for sort join : " + (System.currentTimeMillis() - t1));

    Assert.assertEquals(hashResult.size(), sortResult.size());

    hashResult.sort(JoinTestUtils.getJoinedTupleComparator());
    sortResult.sort(JoinTestUtils.getJoinedTupleComparator());

    for (int i = 0; i < hashResult.size(); i++) {
      Assert.assertEquals(hashResult.get(i), sortResult.get(i));
    }
  }

  @Test
  public void leftJoinTest() {
    List<Tuple> leftRelation = randomData();
    List<Tuple> rightRelation = randomData();

    KeyComparatorWrapper intComparator = intComparator();

    long t1 = System.currentTimeMillis();
    List<Object> hashResult = HashJoinUtils.leftOuterJoin(leftRelation, rightRelation,
        intComparator);
    LOG.info("Time for hash join : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    List<Object> sortResult = SortJoinUtils.leftOuterJoin(leftRelation, rightRelation,
        intComparator);
    LOG.info("Time for sort join : " + (System.currentTimeMillis() - t1));

    Assert.assertEquals(hashResult.size(), sortResult.size());

    hashResult.sort(JoinTestUtils.getJoinedTupleComparator());
    sortResult.sort(JoinTestUtils.getJoinedTupleComparator());

    for (int i = 0; i < hashResult.size(); i++) {
      Assert.assertEquals(hashResult.get(i), sortResult.get(i));
    }
  }

  @Test
  public void rightJoinTest() {
    List<Tuple> leftRelation = randomData();
    List<Tuple> rightRelation = randomData();

    KeyComparatorWrapper intComparator = intComparator();

    long t1 = System.currentTimeMillis();
    List<Object> hashResult = HashJoinUtils.rightOuterJoin(leftRelation, rightRelation,
        intComparator);
    LOG.info("Time for hash join : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    List<Object> sortResult = SortJoinUtils.rightOuterJoin(leftRelation, rightRelation,
        intComparator);
    LOG.info("Time for sort join : " + (System.currentTimeMillis() - t1));

    Assert.assertEquals(hashResult.size(), sortResult.size());

    hashResult.sort(JoinTestUtils.getJoinedTupleComparator());
    sortResult.sort(JoinTestUtils.getJoinedTupleComparator());

    for (int i = 0; i < hashResult.size(); i++) {
      Assert.assertEquals(hashResult.get(i), sortResult.get(i));
    }
  }
}
