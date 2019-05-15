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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public class AggregatorListTest extends BaseSerializeTest {
  @Test
  public void testAggregatedObject() {
    int numBuffers = 16;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      data.add("I " + i);
    }
    Tuple tuple = new Tuple(1, data, MessageTypes.INTEGER, MessageTypes.OBJECT);

    InMessage inMessage = keyedSingleValueCase(numBuffers, size, MessageTypes.OBJECT,
        MessageTypes.INTEGER, tuple);
    Tuple result = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((int) result.getKey(), 1);
    List<Object> resultList = (List<Object>) tuple.getValue();
    Assert.assertEquals(resultList, data);
  }

  @Test
  public void testAggregatedList() {
    int numBuffers = 16;
    int size = 1000;

    List<Object> tuples = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      List<Object> data = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        data.add("I " + i);
      }
      Tuple tuple = new Tuple(1, data, MessageTypes.INTEGER, MessageTypes.OBJECT);
      tuples.add(tuple);
    }

    try {
      InMessage inMessage = keyedListValueCase(numBuffers, size, tuples, MessageTypes.OBJECT,
          MessageTypes.INTEGER);
      Assert.fail();
    } catch (ClassCastException e) {
      Assert.assertTrue(true);
    }

    List<Object> aTuples = new AggregatedObjects<>();
    for (int j = 0; j < 10; j++) {
      List<Object> data = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        data.add("I " + i);
      }
      Tuple tuple = new Tuple(1, data, MessageTypes.INTEGER, MessageTypes.OBJECT);
      aTuples.add(tuple);
    }

    try {
      InMessage inMessage = keyedListValueCase(numBuffers, size, aTuples, MessageTypes.OBJECT,
          MessageTypes.INTEGER);
      Assert.assertTrue(true);
    } catch (ClassCastException e) {
      Assert.fail();
    }
  }
}
