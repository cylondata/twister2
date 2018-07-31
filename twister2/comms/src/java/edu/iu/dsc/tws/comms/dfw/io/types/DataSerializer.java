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
import java.nio.ShortBuffer;
import java.util.DoubleSummaryStatistics;
import java.util.List;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.data.memory.MemoryManagerContext;

public final class DataSerializer {
  private DataSerializer() {
  }

  /**
   * Serialize the key and set it to the state
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

  /**
   * Copy the key to the buffer
   */
  public static boolean copyDataToBuffer(Object data, MessageType keyType,
                                         ByteBuffer targetBuffer, SerializeState state,
                                         KryoSerializer serializer) {
    // LOG.info(String.format("%d copy key: %d", executor, targetBuffer.position()));
    switch (keyType) {
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

  private static boolean copyLong(long[] data, ByteBuffer targetBuffer, SerializeState state) {
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

  private static boolean copyShort(short[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    int remainingToCopy = data.length * Short.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / Short.BYTES;
    // copy
    int offSet = bytesCopied / Short.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putShort(data[i + offSet]);
    }

    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Short.BYTES);
  }

  private static boolean copyDoubles(double[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * Double.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / Double.BYTES;
    // copy
    int offSet = bytesCopied / Double.BYTES;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putDouble(data[i + offSet]);
    }

    return updateState(state, totalBytes, canCopy, remainingToCopy, bytesCopied, Double.BYTES);
  }

  private static boolean copyIntegers(int[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * Integer.BYTES - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / Integer.BYTES;
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
  private static boolean copyDataBytes(ByteBuffer targetBuffer, SerializeState state) {
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
}
