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
package edu.iu.dsc.tws.comms.mpi.io.types;

import java.nio.ByteBuffer;
import java.util.List;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.io.SerializeState;
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
        return ((int[]) content).length * 4;
      case SHORT:
        return ((short[]) content).length * 2;
      case LONG:
        return ((long[]) content).length * 8;
      case DOUBLE:
        return ((double[]) content).length * 8;
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

  /*public byte[] getMessageBytes() throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (final Map.Entry<Short,byte[]> entry : myMap.entrySet()) {
      baos.write(entry.getValue());
    }
    baos.flush();
    return baos.toByteArray();
  }*/

  /**
   * get serialized data
   */
  public static void getserializedData(Object content, MessageType messageType,
                                       SerializeState state,
                                       KryoSerializer serializer, ByteBuffer targetBuffer) {
    ByteBuffer dataBuffer;
    switch (messageType) {
      case INTEGER:
        int[] intdata = (int[]) content;
        copyIntegers(intdata, targetBuffer);
        targetBuffer.flip();
        break;
      case SHORT:
        short[] shortdata = (short[]) content;
        copyShorts(shortdata, targetBuffer);
        targetBuffer.flip();
        break;
      case LONG:
        long[] longdata = (long[]) content;
        copyLongs(longdata, targetBuffer);
        targetBuffer.flip();
        break;
      case DOUBLE:
        double[] doubledata = (double[]) content;
        copyDoubles(doubledata, targetBuffer);
        targetBuffer.flip();
        break;
      case OBJECT:
        if (state.getData() == null) {
          byte[] serialize = serializer.serialize(content);
          state.setData(serialize);
        }
        targetBuffer.putInt(state.getData().length);
        targetBuffer.put(state.getData());
        targetBuffer.flip();
        break;
      case BYTE:
        if (state.getData() == null) {
          state.setData((byte[]) content);
        }
        targetBuffer.putInt(state.getData().length);
        targetBuffer.put(state.getData());
        targetBuffer.flip();
        break;
      case STRING:
        if (state.getData() == null) {
          byte[] serialize = ((String) content).getBytes(MemoryManagerContext.DEFAULT_CHARSET);
          state.setData(serialize);
        }
        targetBuffer.putInt(state.getData().length);
        targetBuffer.put(state.getData());
        targetBuffer.flip();
        break;
      default:
    }
  }

  @SuppressWarnings("unchecked")
  public static List<byte[]> getserializedMultiData(Object object, MessageType messageType,
                                                    SerializeState serializationState,
                                                    KryoSerializer kryoSerializer) {
    switch (messageType) {
      case MULTI_FIXED_BYTE:
        return (List<byte[]>) object;
      default:
        return null;
    }
  }

  private static void copyIntegers(int[] data, ByteBuffer dataBuffer) {
    for (int i : data) {
      dataBuffer.putInt(i);
    }
  }

  private static void copyShorts(short[] data, ByteBuffer dataBuffer) {
    for (short i : data) {
      dataBuffer.putShort(i);
    }
  }

  private static void copyLongs(long[] data, ByteBuffer dataBuffer) {
    for (long i : data) {
      dataBuffer.putLong(i);
    }
  }

  private static void copyDoubles(double[] data, ByteBuffer dataBuffer) {
    for (double i : data) {
      dataBuffer.putDouble(i);
    }
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

    int remainingToCopy = data.length * 8 - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / 8;
    // copy
    int offSet = bytesCopied / 8;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putLong(data[i + offSet]);
    }
    totalBytes += canCopy * 8;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * 8) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * 8 + bytesCopied);
      return false;
    }
  }

  private static boolean copyShort(short[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    int remainingToCopy = data.length * 2 - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / 2;
    // copy
    int offSet = bytesCopied / 2;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putShort(data[i + offSet]);
    }
    totalBytes += canCopy * 2;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * 2) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * 2 + bytesCopied);
      return false;
    }
  }

  private static boolean copyDoubles(double[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * 8 - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / 8;
    // copy
    int offSet = bytesCopied / 8;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putDouble(data[i + offSet]);
    }
    totalBytes += canCopy * 8;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * 8) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * 8 + bytesCopied);
      return false;
    }
  }

  private static boolean copyIntegers(int[] data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();
    int remainingToCopy = data.length * 4 - bytesCopied;
    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity) / 4;
    // copy
    int offSet = bytesCopied / 4;
    for (int i = 0; i < canCopy; i++) {
      targetBuffer.putInt(data[i + offSet]);
    }
    totalBytes += canCopy * 4;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * 4) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * 4 + bytesCopied);
      return false;
    }
  }

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
}
