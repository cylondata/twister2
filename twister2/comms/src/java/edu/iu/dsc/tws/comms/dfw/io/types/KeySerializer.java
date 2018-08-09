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

public final class KeySerializer {
  private KeySerializer() {
  }

  /**
   * Calculates the length of the key based on the type of key. If the key is not a primitive
   * type this method will serialize the key object and save it in the {@code state}
   * @param key the key of which the length is calculated
   * @param type the type of the key
   * @param state the state object that records the state of the message
   * @param serializer the serializer to be used if the object needs to be serialized
   * @return the length of the key in BYTES
   */
  public static int serializeKey(Object key, MessageType type,
                                 SerializeState state, KryoSerializer serializer) {
    switch (type) {
      case INTEGER:
        return Integer.BYTES;
      case SHORT:
        return Short.BYTES;
      case LONG:
        return Long.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case OBJECT:
        if (state.getKey() == null) {
          byte[] serialize = serializer.serialize(key);
          state.setKey(serialize);
        }
        return state.getKey().length;
      case BYTE:
        if (state.getKey() == null) {
          state.setKey((byte[]) key);
        }
        return state.getKey().length;
      case STRING:
        if (state.getKey() == null) {
          state.setKey(((String) key).getBytes());
        }
        return state.getKey().length;
      case MULTI_FIXED_BYTE:
        if (state.getKey() == null) {
          state.setKey(getMultiBytes(key));
        }
        return state.getKey().length;
      default:
        break;
    }
    return 0;
  }

  /**
   * Copys the key to the buffer that is passed to this method. If the object has been already been
   * serilized then it will be retrieved from the state object.
   * @param key the key to be copied
   * @param keyType the type of the key
   * @param targetBuffer the buffer to which the key will be copied
   * @param state the state object that holds the state of the message
   * @param serializer the serializer to be used if the object needs to be serialized
   * @return true if the key was copied to the buffer successfully or false otherwise
   */
  @SuppressWarnings("unchecked")
  public static boolean copyKeyToBuffer(Object key, MessageType keyType,
                                        ByteBuffer targetBuffer, SerializeState state,
                                        KryoSerializer serializer) {
    // LOG.info(String.format("%d copy key: %d", executor, targetBuffer.position()));
    switch (keyType) {
      case INTEGER:
        if (targetBuffer.remaining() > Integer.BYTES) {
          targetBuffer.putInt((Integer) key);
          state.setTotalBytes(state.getTotalBytes() + Integer.BYTES);
          state.setCurretHeaderLength(state.getCurretHeaderLength() + Integer.BYTES);
          state.setKeySize(Integer.BYTES);
          return true;
        }
        break;
      case SHORT:
        if (targetBuffer.remaining() > Short.BYTES) {
          targetBuffer.putShort((short) key);
          state.setTotalBytes(state.getTotalBytes() + Short.BYTES);
          state.setCurretHeaderLength(state.getCurretHeaderLength() + Short.BYTES);
          state.setKeySize(Short.BYTES);
          return true;
        }
        break;
      case LONG:
        if (targetBuffer.remaining() > Long.BYTES) {
          targetBuffer.putLong((Long) key);
          state.setTotalBytes(state.getTotalBytes() + Long.BYTES);
          state.setCurretHeaderLength(state.getCurretHeaderLength() + Long.BYTES);
          state.setKeySize(Long.BYTES);
          return true;
        }
        break;
      case DOUBLE:
        if (targetBuffer.remaining() > Double.BYTES) {
          targetBuffer.putDouble((Double) key);
          state.setTotalBytes(state.getTotalBytes() + Double.BYTES);
          state.setCurretHeaderLength(state.getCurretHeaderLength() + Double.BYTES);
          state.setKeySize(Double.BYTES);
          return true;
        }
        break;
      case OBJECT:
        if (state.getKey() == null) {
          byte[] serialize = serializer.serialize(key);
          state.setKey(serialize);
        }
        return copyKeyBytes(targetBuffer, state);
      case BYTE:
        if (state.getKey() == null) {
          state.setKey((byte[]) key);
        }
        return copyKeyBytes(targetBuffer, state);
      case STRING:
        if (state.getKey() == null) {
          state.setKey(((String) key).getBytes());
        }
        return copyKeyBytes(targetBuffer, state);
      case MULTI_FIXED_BYTE:
        if (state.getKey() == null) {
          state.setKey(getMultiBytes(key));
        }
        return copyMultiKeyBytes(targetBuffer, state, ((List<byte[]>) key).size());
      default:
        break;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static boolean copyKeyBytes(ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    byte[] key = state.getKey();
    if (bytesCopied == 0 && remainingCapacity > Integer.BYTES) {
      targetBuffer.putInt(key.length);
      totalBytes += Integer.BYTES;
    } else {
      return false;
    }

    int remainingToCopy = key.length - bytesCopied;
    int canCopy = remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity;
    // copy
    targetBuffer.put(key, bytesCopied, canCopy);
    totalBytes += canCopy;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we will use this size later
    state.setKeySize(key.length + Integer.BYTES);
    // we copied everything
    if (canCopy == remainingToCopy) {
      state.setKey(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy + bytesCopied);
      return false;
    }
  }

  private static boolean copyMultiKeyBytes(ByteBuffer targetBuffer, SerializeState state,
                                           int size) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    byte[] key = state.getKey();
    if (bytesCopied == 0 && remainingCapacity > Integer.BYTES * 2) {
      //the number of key in the multi key
      targetBuffer.putInt(size);
      targetBuffer.putInt(key.length);
      totalBytes += Integer.BYTES * 2;
    } else {
      return false;
    }

    int remainingToCopy = key.length - bytesCopied;
    int canCopy = remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity;
    // copy
    targetBuffer.put(key, bytesCopied, canCopy);
    totalBytes += canCopy;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we will use this size later
    state.setKeySize(key.length + Integer.BYTES);
    // we copied everything
    if (canCopy == remainingToCopy) {
      state.setKey(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy + bytesCopied);
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static byte[] getMultiBytes(Object key) {
    List<byte[]> keys = (List<byte[]>) key;
    byte[] keyBytes = new byte[keys.size() * keys.get(0).length];
    int offset = 0;
    for (byte[] bytes : keys) {
      System.arraycopy(bytes, 0, keyBytes, offset, bytes.length);
      offset += bytes.length;
    }
    return keyBytes;
  }
}
