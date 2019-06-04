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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public class KeyedSerializerLargeTest extends BaseSerializeTest {
  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListByteMessage() {
    int numBuffers = 32;
    int size = 1024000;

    for (int numObjects = 15000; numObjects < 16000; numObjects++) {
      System.out.println("Starting test : " + numObjects);
      List<Object> data = new AggregatedObjects<>();
      for (int i = 0; i < numObjects; i++) {
        Object o = createKeyedData(90, MessageTypes.BYTE_ARRAY, 10, MessageTypes.BYTE_ARRAY);
        data.add(o);
      }

      InMessage inMessage = keyedListValueCase(numBuffers, size, data,
          MessageTypes.BYTE_ARRAY, MessageTypes.BYTE_ARRAY);

      List<Object> result = (List<Object>) inMessage.getDeserializedData();

      Assert.assertEquals(numObjects, result.size());

      for (int i = 0; i < result.size(); i++) {
        Tuple deserializedData = (Tuple) result.get(i);
        Tuple d = (Tuple) data.get(i);

        Assert.assertArrayEquals((byte[]) deserializedData.getKey(), (byte[]) ((Tuple) d).getKey());
        Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
            (byte[]) ((Tuple) d).getValue());
      }
    }
  }
}
