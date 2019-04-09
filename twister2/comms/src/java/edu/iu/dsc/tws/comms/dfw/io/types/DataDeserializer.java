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
package edu.iu.dsc.tws.comms.dfw.io.types;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;

public final class DataDeserializer {
  private DataDeserializer() {
  }

  /**
   * Deserialize's the given data in the ByteBuffer based on the dataType
   *
   * @param dataType the type of the the object in the buffer
   * @param deserializer the deserializer to be used for types other than primitives
   * @param os the buffer that contains the data
   * @param dataSize the length of the current data object to be extracted
   * @return the deserialized object
   */
  public static Object deserialize(MessageType dataType, KryoSerializer deserializer,
                                   ByteBuffer os, int dataSize) {
    Object data = null;
    if (dataType == MessageType.OBJECT) {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = deserializer.deserialize(bytes);
    } else if (dataType == MessageType.BYTE) {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = bytes;
    } else if (dataType == MessageType.DOUBLE) {
      double[] bytes = new double[dataSize / Double.BYTES];
      for (int i = 0; i < dataSize / Double.BYTES; i++) {
        bytes[i] = os.getDouble();
      }
      data = bytes;
    } else if (dataType == MessageType.INTEGER) {
      int[] bytes = new int[dataSize / Integer.BYTES];
      for (int i = 0; i < dataSize / Integer.BYTES; i++) {
        bytes[i] = os.getInt();
      }
      data = bytes;
    } else if (dataType == MessageType.LONG) {
      long[] bytes = new long[dataSize / Long.BYTES];
      for (int i = 0; i < dataSize / Long.BYTES; i++) {
        bytes[i] = os.getLong();
      }
      data = bytes;
    } else if (dataType == MessageType.SHORT) {
      short[] bytes = new short[dataSize / Short.BYTES];
      for (int i = 0; i < dataSize / Short.BYTES; i++) {
        bytes[i] = os.getShort();
      }
      data = bytes;
    } else if (dataType == MessageType.CHAR) {
      char[] bytes = new char[dataSize];
      for (int i = 0; i < dataSize; i++) {
        bytes[i] = os.getChar();
      }
      data = bytes;
    } else {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = deserializer.deserialize(bytes);
    }
    return data;
  }
}
