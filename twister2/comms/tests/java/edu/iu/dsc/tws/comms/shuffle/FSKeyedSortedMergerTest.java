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
package edu.iu.dsc.tws.comms.shuffle;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.types.primitive.IntegerArrayPacker;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.util.CommonThreadPool;
import edu.iu.dsc.tws.api.util.KryoSerializer;

@SuppressWarnings({"unchecked", "rawtypes"})
public class FSKeyedSortedMergerTest {
  private static final Logger LOG = Logger.getLogger(FSMergerTest.class.getName());

  private FSKeyedSortedMerger2 fsMerger;

  private KryoSerializer serializer;

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void before() throws Exception {
    fsMerger = new FSKeyedSortedMerger2(10000000, 10000, "/tmp",
        "fskeyedsortedmerger", MessageTypes.INTEGER, MessageTypes.INTEGER_ARRAY,
        new IntComparator(), 0, true, 2);
    serializer = new KryoSerializer();
    CommonThreadPool.init(Config.newBuilder().build());
  }

  private class IntComparator implements Comparator<Integer> {

    @Override
    public int compare(Integer o1, Integer o2) {
      return o1 - o2;
    }
  }

  @After
  public void after() throws Exception {
    fsMerger.clean();
  }

  private class KeyComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      int[] val1 = (int[]) o1;
      int[] val2 = (int[]) o2;
      return Integer.compare(val1[0], val2[0]);
    }
  }

  @Test
  public void testStart() throws Exception {
    int dataLength = 1024;
    int noOfKeys = 100;
    int dataForEachKey = 10;
    int[] data = new int[dataLength];
    Arrays.fill(data, 1);
    byte[] byteType = IntegerArrayPacker.getInstance().packToByteArray(data);
    for (int i = 0; i < noOfKeys; i++) {
      for (int j = 0; j < dataForEachKey; j++) {
        fsMerger.add(i, byteType, byteType.length);
      }
      fsMerger.run();
    }

    fsMerger.switchToReading();

    Iterator<Object> it = fsMerger.readIterator();
    int count = 0;
    Set<Integer> set = new HashSet<>();
    int current = 0;
    while (it.hasNext()) {
      Tuple val = (Tuple) it.next();
      int k = (int) val.getKey();
      if (k < current) {
        Assert.fail("Wrong order");
      }
      current = k;
      if (set.contains(k)) {
        Assert.fail("Duplicate value");
      }
      set.add(k);
      //data check
      Iterator dataIt = (Iterator) val.getValue();
      int dataCount = 0;
      while (dataIt.hasNext()) {
        int[] arr = (int[]) dataIt.next();
        if (arr.length != dataLength) {
          Assert.fail("Data sizes mismatch");
        }
        dataCount++;
      }
      if (dataCount != dataForEachKey) {
        Assert.fail("Invalid amount of data arrays for key");
      }
      count++;
    }
    if (count != noOfKeys) {
      Assert.fail("Count =  " + count);
    }
  }
}
