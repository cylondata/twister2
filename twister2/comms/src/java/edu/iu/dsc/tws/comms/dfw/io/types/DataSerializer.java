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

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.data.memory.MemoryManagerContext;

public final class DataSerializer {
  private DataSerializer() {
  }

  /**
   * This method returns the length of the message object in bytes. If the message is of a primitive
   * type the length is calculated and returned (no serialization happens). If the message needs to
   * be serialized the message is serialized and the length is returned. The serialized content is
   * saved in the state object to make sure the same message is not serialized twice
   *
   * @param content the message that needs to be serialized
   * @param type the type of the message
   * @param state the state object used to save the message state
   * @param serializer the serializer to be used if the object needs to be serialized
   * @return the length of the message
   */
  public static int serializeData(Object content, MessageType type,
                                  SerializeState state, KryoSerializer serializer) {
    switch (type) {
      case INTEGER:
        return ((int[]) content).length * Integer.BYTES;
      case SHORT:
        return ((short[]) content).length * Short.BYTES;
      case LONG:
        return ((long[]) content).length * Long.BYTES;
      case DOUBLE:
        return ((double[]) content).length * Double.BYTES;
      case OBJECT:
        if (state.getData() == null) {
          byte[] serialize = serializer.serialize(content);
          state.setData(serialize);
        }
        return state.getData().length;
      case BYTE:
        if (state.getData() == null) {
          state.setData((byte[]) content);
        }
        return state.getData().length;
      case STRING:
        if (state.getData() == null) {
          byte[] serialize = ((String) content).getBytes(MemoryManagerContext.DEFAULT_CHARSET);
          state.setData(serialize);
        }
        return state.getData().length;
      case MULTI_FIXED_BYTE:
        if (state.getData() == null) {
          state.setData(getBytes(content));
        }
        return state.getData().length;
      default:
        break;
    }
    return 0;
  }

  /**
   * Copies the serialized message into the target buffer that is passed to the method
   * if the message is not already serialized, the message will be serialized before it is copied
   * to the buffer.
   *
   * @param data message that needs to be copied to the buffer
   * @param messageType type of the message
   * @param targetBuffer target buffer to which the data is copied
   * @param state state object that keeps the state of the message
   * @param serializer the serializer to be used if the object needs to be serialized
   * @return true if the message is copied successfully, false otherwise.
   */
  public static boolean copyDataToBuffer(Object data, MessageType messageType,
                                         ByteBuffer targetBuffer, SerializeState state,
                                         KryoSerializer serializer) {
    // LOG.info(String.format("%d copy key: %d", executor, targetBuffer.position()));
    switch (messageType) {
      case INTEGER:
        return copyIntegers((int[]) data, targetBuffer, state);
      case SHORT:
        return copyShort((short[]) data, targetBuffer, state);
      case LONG:
        return copyLong((long[]) data, targetBuffer, state);
      case DOUBLE:
        return copyDoubles((double[]) data, targetBuffer, state);
      case OBJECT:
        if (state.getData() == null) {
          byte[] serialize = serializer.serialize(data);
          state.setData(serialize);
        }
        return copyDataBytes(targetBuffer, state);
      case BYTE:
        if (state.getData() == null) {
          state.setData((byte[]) data);
        }
        return copyDataBytes(targetBuffer, state);
      case STRING:
        if (state.getData() == null) {
          state.setData(((String) data).getBytes(MemoryManagerContext.DEFAULT_CHARSET));
        }
        return copyDataBytes(targetBuffer, state);
      case MULTI_FIXED_BYTE:
        if (state.getData() == null) {
          state.setData(getBytes(data));
        }
        return copyDataBytes(targetBuffer, state);
      default:
        break;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static byte[] getBytes(Object data) {
    List<byte[]> dataValues = (List<byte[]>) data;
    byte[] dataBytes = new byte[dataValues.size() * dataValues.get(0).length];
    int offset = 0;
    for (byte[] bytes : dataValues) {
      System.arraycopy(bytes, 0, dataBytes, offset, bytes.length);
      offset += bytes.length;
    }
    return dataBytes;
    //TODO check if the commented getMessageBytes is faster
  }

  public static boolean copyLong(long[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    int remainingToCopy = data.length * Long.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / 8;
    // copy
    int offSet = bytesCopied / Long.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putLong(data[i + offSet]);
    }

    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Long.BYTES);
  }

  public static boolean copyShort(short[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    int remainingToCopy = data.length * Short.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy
        : remainingCapacity) / Short.BYTES;
    // copy
    int offSet = bytesCopied / Short.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putShort(data[i + offSet]);
    }

    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Short.BYTES);
  }

  public static boolean copyDoubles(double[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * Double.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy
        : remainingCapacity) / Double.BYTES;
    // copy
    int offSet = bytesCopied / Double.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putDouble(data[i + offSet]);
    }

    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Double.BYTES);
  }

  public static boolean copyIntegers(int[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * Integer.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy
        : remainingCapacity) / Integer.BYTES;
    // copy
    int offSet = bytesCopied / Integer.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putInt(data[i + offSet]);
    }
    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Integer.BYTES);
  }

  /**
   * copys the given data in the state into the target buffer, if the the buffer is not big
   * enough it will make a partial copy and return false
   *
   * @return true if all the data was copied and false if only a part was copied
   */
  public static boolean copyDataBytes(ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    byte[] data = state.getData();
    int remainingToCopy = data.length - bytesCopied;
    int canCopy = remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity;
    // copy
    targetBuffer.put(data, bytesCopied, canCopy);
    totalBytes += canCopy;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);

    // we will use this size later
    if (canCopy == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy + bytesCopied);
      return false;
    }
  }

  private static boolean updateState(SerializeState state, int curTotalBytes, int canCopy,
                                     int remainingToCopy, int bytesCopied, int typeLength) {
    int totalBytes = curTotalBytes + canCopy * typeLength;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * typeLength) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * typeLength + bytesCopied);
      return false;
    }
  }

  public static byte[] serialize(Object data, KryoSerializer serializer) {

    if (data instanceof byte[]) {
      return (byte[]) data;
    } else if (data instanceof short[]) {
      return convertShortsToBytes((short[]) data);
    } else if (data instanceof int[]) {
      return convertIntegersToBytes((int[]) data);
    } else if (data instanceof long[]) {
      return convertLongsToBytes((long[]) data);
    } else if (data instanceof double[]) {
      return convertDoublesToBytes((double[]) data);
    } else if (data instanceof float[]) {
      return convertFloatsToBytes((float[]) data);
    } else if (data instanceof char[]) {
      //TODO: do we need to convert for char array?
      return convertCharsToBytes((char[]) data);
    } else {
      return serializer.serialize(data);
    }
  }

  //TODO: using bitwise operations are much faster than using the ByteBuffer, Need to convert
  //TODO: the other methods to use bitwise operations as done for int arrays
  public static byte[] convertIntegersToBytes(int[] integers) {
    byte[] outputBytes = new byte[integers.length * Integer.BYTES];

    for (int m = 0, k = 0; m < integers.length; m++) {
      int integerTemp = integers[m];
      for (int j = 0; j < 4; j++, k++) {
        outputBytes[k] = (byte) ((integerTemp >> (8 * (3 - j))));
      }
    }
    return outputBytes;
  }

  private static byte[] convertShortsToBytes(short[] shorts) {
    byte[] outputBytes = new byte[shorts.length * Short.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(outputBytes);
    for (short s : shorts) {
      bb.putShort(s);
    }
    return outputBytes;
  }

  private static byte[] convertDoublesToBytes(double[] doubles) {
    byte[] outputBytes = new byte[doubles.length * Double.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(outputBytes);
    for (double d : doubles) {
      bb.putDouble(d);
    }
    return outputBytes;
  }

  private static byte[] convertLongsToBytes(long[] longs) {
    byte[] outputBytes = new byte[longs.length * Long.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(outputBytes);
    for (long l : longs) {
      bb.putLong(l);
    }
    return outputBytes;
  }

  private static byte[] convertFloatsToBytes(float[] floats) {
    byte[] outputBytes = new byte[floats.length * Float.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(outputBytes);
    for (float f : floats) {
      bb.putFloat(f);
    }
    return outputBytes;
  }

  private static byte[] convertCharsToBytes(char[] chars) {
    byte[] outputBytes = new byte[chars.length];
    ByteBuffer bb = ByteBuffer.wrap(outputBytes);
    for (char c : chars) {
      bb.putChar(c);
    }
    return outputBytes;
  }
}
