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
package edu.iu.dsc.tws.comms.dfw.io.types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.ByteArrayInputStream;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;

public final class DataDeserializer {
  private DataDeserializer() {
  }

  /**
   * used when there are more than 1 data object
   * types other than multi types return as normal
   */
  /**
   * Deserialize's the message from the list of data buffers.Used when there are more than 1
   * data object types other than multi types return as normal
   *
   * @param buffers the buffer which contain the message data
   * @param length the length of the object that needs to be extracted
   * @param deserializer the deserializer to be used if the object needs to be deserialized
   * @param type the type of the message
   * @param count the number of data objects in multi types
   */
  public static Object deserializeData(List<DataBuffer> buffers, int length,
                                       KryoSerializer deserializer, MessageType type, int count) {

    switch (type) {
      case INTEGER:
        return deserializeInteger(buffers, length);
      case DOUBLE:
        return deserializeDouble(buffers, length);
      case SHORT:
        return deserializeShort(buffers, length);
      case BYTE:
        return deserializeBytes(buffers, length);
      case OBJECT:
        return deserializeObject(buffers, length, deserializer);
      case MULTI_FIXED_BYTE:
        return deserializeMultiBytes(buffers, length, count);
      default:
        break;
    }
    return null;
  }

  /**
   * Deserialize's the message from the list of data buffers.
   *
   * @param buffers the buffer which contain the message data
   * @param length the length of the object that needs to be extracted
   * @param deserializer the deserializer to be used if the object needs to be deserialized
   * @param type the type of the message
   * @return the object that was deserialized
   */
  public static Object deserializeData(List<DataBuffer> buffers, int length,
                                       KryoSerializer deserializer, MessageType type) {

    switch (type) {
      case INTEGER:
        return deserializeInteger(buffers, length);
      case DOUBLE:
        return deserializeDouble(buffers, length);
      case SHORT:
        return deserializeShort(buffers, length);
      case BYTE:
        return deserializeBytes(buffers, length);
      case OBJECT:
        return deserializeObject(buffers, length, deserializer);
      default:
        break;
    }
    return null;
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

  public static List<byte[]> getAsByteArray(List<DataBuffer> buffers, int length,
                                            MessageType type, int count) {
    List<byte[]> data = new ArrayList<>();
    int singleDataLength = length / count;
    for (int i = 0; i < count; i++) {
      data.add(getAsByteArray(buffers, singleDataLength, type));
    }
    return data;
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

  private static byte[] deserializeBytes(List<DataBuffer> buffers, int length) {
    byte[] returnBytes = new byte[length];
    int bufferIndex = 0;
    int copiedBytes = 0;
    while (copiedBytes < length) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      int remainingToCopy = length - copiedBytes;
      if (remaining > remainingToCopy) {
        byteBuffer.get(returnBytes, copiedBytes, remainingToCopy);
        copiedBytes += remainingToCopy;
      } else {
        byteBuffer.get(returnBytes, copiedBytes, remaining);
        copiedBytes += remaining;
        bufferIndex++;
      }
    }
    return returnBytes;
  }

  private static Object deserializeMultiBytes(List<DataBuffer> buffers, int length, int count) {
    List<byte[]> data = new ArrayList<>();
    int singleDataLength = length / count;
    for (int i = 0; i < count; i++) {
      data.add(deserializeBytes(buffers, singleDataLength));
    }
    return data;
  }


  public static double[] deserializeDouble(List<DataBuffer> buffers, int byteLength) {
    int noOfDoubles = byteLength / Double.BYTES;
    double[] returnInts = new double[noOfDoubles];
    int bufferIndex = 0;
    for (int i = 0; i < noOfDoubles; i++) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining >= Double.BYTES) {
        returnInts[i] = byteBuffer.getDouble();
      } else {
        bufferIndex = getReadBuffer(buffers, Double.BYTES, bufferIndex);
        if (bufferIndex < 0) {
          throw new RuntimeException("We should always have the doubles");
        }
      }
    }
    return returnInts;
  }

  public static int[] deserializeInteger(List<DataBuffer> buffers, int byteLength) {
    int noOfInts = byteLength / Integer.BYTES;
    int[] returnInts = new int[noOfInts];
    int bufferIndex = 0;
    for (int i = 0; i < noOfInts; i++) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining >= Integer.BYTES) {
        returnInts[i] = byteBuffer.getInt();
      } else {
        bufferIndex = getReadBuffer(buffers, Integer.BYTES, bufferIndex);
        if (bufferIndex < 0) {
          throw new RuntimeException("We should always have the ints");
        }
      }
    }
    return returnInts;
  }

  public static short[] deserializeShort(List<DataBuffer> buffers, int byteLength) {
    int noOfShorts = byteLength / Short.BYTES;
    short[] returnShorts = new short[noOfShorts];
    int bufferIndex = 0;
    for (int i = 0; i < noOfShorts; i++) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining >= Short.BYTES) {
        returnShorts[i] = byteBuffer.getShort();
      } else {
        bufferIndex = getReadBuffer(buffers, Short.BYTES, bufferIndex);
        if (bufferIndex < 0) {
          throw new RuntimeException("We should always have the shorts");
        }
      }
    }
    return returnShorts;
  }

  public static long[] deserializeLong(List<DataBuffer> buffers, int byteLength) {
    int noOfLongs = byteLength / Long.BYTES;
    long[] returnLongs = new long[noOfLongs];
    int bufferIndex = 0;
    for (int i = 0; i < noOfLongs; i++) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining >= Long.BYTES) {
        returnLongs[i] = byteBuffer.getLong();
      } else {
        bufferIndex = getReadBuffer(buffers, Long.BYTES, bufferIndex);
        if (bufferIndex < 0) {
          throw new RuntimeException("We should always have the longs");
        }
      }
    }
    return returnLongs;
  }

  private static int getReadBuffer(List<DataBuffer> bufs, int size,
                                   int currentBufferIndex) {

    for (int i = currentBufferIndex; i < bufs.size(); i++) {
      ByteBuffer byteBuffer = bufs.get(i).getByteBuffer();
      // now check if we need to go to the next buffer
      if (byteBuffer.remaining() > size) {
        // if we are at the end we need to move to next
        byteBuffer.rewind();
        return i;
      }
    }
    return -1;
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
  public static Object deserialize(MessageType dataType, KryoMemorySerializer deserializer,
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
  public static Object deserialize(MessageType dataType, KryoMemorySerializer deserializer,
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
