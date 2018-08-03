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

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.ByteArrayInputStream;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class DataDeserializer {
  private DataDeserializer() {
  }

  /**
   * used when there are more than 1 data object
   * types other than multi types return as normal
   */
  public static Object deserializeData(List<DataBuffer> buffers, int length,
                                       KryoSerializer serializer, MessageType type, int count,
                                       int flag) {
    if ((flag & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
      deserializeObject(buffers, length, serializer);
    } else {
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
          return deserializeObject(buffers, length, serializer);
        case MULTI_FIXED_BYTE:
          return deserializeMultiBytes(buffers, length, count);
        default:
          break;
      }
    }
    return null;
  }

  public static Object deserializeData(List<DataBuffer> buffers, int length,
                                       KryoSerializer serializer, MessageType type, int flag) {

    if ((flag & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
      deserializeObject(buffers, length, serializer);
    } else {
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
          return deserializeObject(buffers, length, serializer);
        default:
          break;
      }
    }
    return null;
  }

  /**
   * get bytes
   */
  public static byte[] getAsByteBuffer(List<DataBuffer> buffers, int length, MessageType type) {
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

  public static List<byte[]> getAsByteBuffer(List<DataBuffer> buffers, int length,
                                             MessageType type, int count) {
    List<byte[]> data = new ArrayList<>();
    int singleDataLength = length / count;
    for (int i = 0; i < count; i++) {
      data.add(getAsByteBuffer(buffers, singleDataLength, type));
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

}
