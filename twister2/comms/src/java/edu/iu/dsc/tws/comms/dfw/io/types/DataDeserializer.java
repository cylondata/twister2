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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.ByteArrayInputStream;

public final class DataDeserializer {
  private DataDeserializer() {
  }

  /**
   * Gets the message in the buffer list as a byte array
   *
   * @param buffers the buffer list that contains the message
   * @param length the length of the message to be retrieved
   * @param type the type of the message
   * @return the message as a byte[]
   */
  public static byte[] getAsByteArray(List<DataBuffer> buffers, int length, MessageType type) {
    //If the message type is object we need to add the length of each object to the
    //bytestream so we can separate objects
    //We will try to reuse this array when possible
    byte[] tempByteArray = new byte[length];
    int canCopy = 0;
    int bufferIndex = 0;
    int copiedBytes = 0;
    ByteBuffer tempbyteBuffer;
    //TODO: need to check if this is correctly copying the data
    //TODO: Also check if the created bytes may be too big
    //TODO: check of MPIBuffer always has the correct size
    while (copiedBytes < length) {
      tempbyteBuffer = buffers.get(bufferIndex).getByteBuffer();
      canCopy = buffers.get(bufferIndex).getSize() - tempbyteBuffer.position();

      //If we don't need all the bytes just take what we want
      if (canCopy + copiedBytes > length) {
        canCopy = length - copiedBytes;
      }

      if (tempByteArray.length < canCopy) {
        //We need a bigger temp array
        tempByteArray = new byte[canCopy];
      }
      tempbyteBuffer.get(tempByteArray, copiedBytes, canCopy);
      copiedBytes += canCopy;
      bufferIndex++;
    }
    return tempByteArray;
  }

  public static Object deserializeObject(List<DataBuffer> buffers, int length,
                                         KryoSerializer serializer) {
    ByteArrayInputStream input = null;
    try {
      input = new ByteArrayInputStream(buffers, length);
      return serializer.deserialize(input);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException ignore) {
        }
      }
    }
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

  /**
   * Deserialize's the given data in the byte[] based on the dataType
   *
   * @param dataType the type of the the object in the buffer
   * @param deserializer the deserializer to be used for types other than primitives
   * @param dataBytes the byte[] that contains the data
   * @return the deserialized object
   */
  public static Object deserialize(MessageType dataType, KryoSerializer deserializer,
                                   byte[] dataBytes) {
    Object data = null;
    ByteBuffer byteBuffer = ByteBuffer.wrap(dataBytes);
    if (dataType == MessageType.OBJECT) {
      data = deserializer.deserialize(dataBytes);
    } else if (dataType == MessageType.BYTE) {
      data = dataBytes;
    } else if (dataType == MessageType.DOUBLE) {
      double[] bytes = new double[dataBytes.length / Double.BYTES];
      for (int i = 0; i < dataBytes.length / Double.BYTES; i++) {
        bytes[i] = byteBuffer.getDouble();
      }
      data = bytes;
    } else if (dataType == MessageType.INTEGER) {
      int[] bytes = new int[dataBytes.length / Integer.BYTES];
      for (int i = 0; i < dataBytes.length / Integer.BYTES; i++) {
        bytes[i] = byteBuffer.getInt();
      }
      data = bytes;
    } else if (dataType == MessageType.LONG) {
      long[] bytes = new long[dataBytes.length / Long.BYTES];
      for (int i = 0; i < dataBytes.length / Long.BYTES; i++) {
        bytes[i] = byteBuffer.getLong();
      }
      data = bytes;
    } else if (dataType == MessageType.SHORT) {
      short[] bytes = new short[dataBytes.length / Short.BYTES];
      for (int i = 0; i < dataBytes.length / Short.BYTES; i++) {
        bytes[i] = byteBuffer.getShort();
      }
      data = bytes;
    } else if (dataType == MessageType.CHAR) {
      char[] bytes = new char[dataBytes.length];
      for (int i = 0; i < dataBytes.length; i++) {
        bytes[i] = byteBuffer.getChar();
      }
      data = bytes;
    } else {
      data = deserializer.deserialize(dataBytes);
    }
    return data;
  }
}
