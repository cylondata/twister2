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
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public final class KeyDeserializer {
  private static final Logger LOG = Logger.getLogger(KeyDeserializer.class.getName());

  private KeyDeserializer() {
  }

  private static byte[] readBytes(List<DataBuffer> buffers, int length) {

    byte[] bytes = new byte[length];
    int currentRead = 0;
    int index = 0;
    while (currentRead < length) {
      ByteBuffer byteBuffer = buffers.get(index).getByteBuffer();
      int remaining = byteBuffer.remaining();
      int needRead = length - currentRead;
      int canRead = remaining > needRead ? needRead : remaining;
      byteBuffer.get(bytes, currentRead, canRead);
      currentRead += canRead;
      index++;
      if (currentRead < length && index >= buffers.size()) {
        throw new RuntimeException("Error in buffer management");
      }
    }
    return bytes;
  }

  /**
   * Deserialize's the given data in the ByteBuffer based on the dataType
   *
   * @param dataType the type of the the object in the buffer
   * @param deserializer the deserializer to be used for types other than primitives
   * @param os the buffer that contains the data
   * @return the deserialized object
   */
  public static Object deserialize(MessageType dataType, KryoSerializer deserializer,
                                   ByteBuffer os, int dataSize) {
    Object data;
    if (dataType == MessageType.OBJECT) {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = deserializer.deserialize(bytes);
    } else if (dataType == MessageType.BYTE) {
      data = os.get();
    } else if (dataType == MessageType.DOUBLE) {
      data = os.getDouble();
    } else if (dataType == MessageType.INTEGER) {
      data = os.getInt();
    } else if (dataType == MessageType.LONG) {
      data = os.getLong();
    } else if (dataType == MessageType.SHORT) {
      data = os.getShort();
    } else if (dataType == MessageType.CHAR) {
      data = os.getChar();
    } else {
      byte[] bytes = new byte[dataSize];
      os.get(bytes);
      data = deserializer.deserialize(bytes);
    }
    return data;
  }
}
