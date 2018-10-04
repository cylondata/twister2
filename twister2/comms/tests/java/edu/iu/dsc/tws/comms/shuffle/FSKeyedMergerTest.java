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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

@SuppressWarnings({"unchecked", "rawtypes"})
public class FSKeyedMergerTest {
  private static final Logger LOG = Logger.getLogger(FSMergerTest.class.getName());

  private FSKeyedMerger fsMerger;

  private Random random;

  private KryoSerializer serializer;

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void before() throws Exception {
    fsMerger = new FSKeyedMerger(1000, 100, "/tmp",
        "fskeyedmerger", MessageType.INTEGER, MessageType.OBJECT);
    random = new Random();
    serializer = new KryoSerializer();
  }

  @After
  public void after() throws Exception {
    fsMerger.clean();
  }

  @Test
  public void testStart() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    for (int i = 0; i < 1000; i++) {
      buffer.clear();
      buffer.putInt(i);
      byte[] serialize = serializer.serialize(i);
      int[] val = {i};
      fsMerger.add(val, serialize, serialize.length);
      fsMerger.run();
    }

    fsMerger.switchToReading();

    Iterator<Object> it = fsMerger.readIterator();
    int count = 0;
    Set<Integer> set = new HashSet<>();
    while (it.hasNext()) {
      LOG.info("Reading value: " + count);
      KeyValue val = (KeyValue) it.next();
      int[] k = (int[]) val.getKey();
      if (set.contains(k[0])) {
        Assert.fail("Duplicate value");
      }
      set.add(k[0]);
      count++;
    }
    if (count != 1000) {
      Assert.fail("Count = " + count);
    }
  }
}
