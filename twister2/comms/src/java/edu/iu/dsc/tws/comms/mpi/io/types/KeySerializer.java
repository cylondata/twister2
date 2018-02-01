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

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.io.SerializeState;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class KeySerializer {
  private KeySerializer() {
  }

  /**
   * Serialize the key and set it to the state
   */
  public static int serializeKey(Object key, MessageType type,
                                 SerializeState state, KryoSerializer serializer) {
    switch (type) {
      case INTEGER:
        return 4;
      case SHORT:
        return 2;
      case LONG:
        return 8;
      case DOUBLE:
        return 8;
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
      default:
        break;
    }
    return 0;
  }

  /**
   * returns the key object as a bytebuffer
   *
   * @param key the key to be serialized
   * @param serializer the serializer used to create the byte stream from the object
   * @return ByteBuffer with the key
   */
  public static ByteBuffer getserializedKey(Object key, SerializeState state,
                                            MessageType keyType, KryoSerializer serializer) {
    ByteBuffer keyBuffer;
    switch (keyType) {
      case INTEGER:
        keyBuffer = ByteBuffer.allocateDirect(4);
        keyBuffer.putInt((Integer) key);
        return keyBuffer;
      case SHORT:
        keyBuffer = ByteBuffer.allocateDirect(2);
        keyBuffer.putShort((Short) key);
        return keyBuffer;
      case LONG:
        keyBuffer = ByteBuffer.allocateDirect(8);
        keyBuffer.putLong((Long) key);
        return keyBuffer;
      case DOUBLE:
        keyBuffer = ByteBuffer.allocateDirect(8);
        keyBuffer.putDouble((Double) key);
        return keyBuffer;
      case OBJECT:
        if (state.getKey() == null) {
          byte[] serialize = serializer.serialize(key);
          state.setKey(serialize);
        }
        keyBuffer = ByteBuffer.allocateDirect(state.getKey().length);
        keyBuffer.put(state.getKey());
        return keyBuffer;
      case BYTE:
        if (state.getKey() == null) {
          state.setKey((byte[]) key);
        }
        keyBuffer = ByteBuffer.allocateDirect(state.getKey().length);
        keyBuffer.put(state.getKey());
        return keyBuffer;
      case STRING:
        if (state.getKey() == null) {
          state.setKey(((String) key).getBytes());
        }
        keyBuffer = ByteBuffer.allocateDirect(state.getKey().length);
        keyBuffer.put(state.getKey());
        return keyBuffer;
      default:
        return null;
    }
  }

  /**
   * Copy the key to the buffer
   */
  public static boolean copyKeyToBuffer(Object key, MessageType keyType,
                                        ByteBuffer targetBuffer, SerializeState state,
                                        KryoSerializer serializer) {
    // LOG.info(String.format("%d copy key: %d", executor, targetBuffer.position()));
    switch (keyType) {
      case INTEGER:
        if (targetBuffer.remaining() > 4) {
          targetBuffer.putInt((Integer) key);
          state.setTotalBytes(state.getTotalBytes() + 4);
          state.setKeySize(4);
          return true;
        }
        break;
      case SHORT:
        if (targetBuffer.remaining() > 2) {
          targetBuffer.putShort((short) key);
          state.setTotalBytes(state.getTotalBytes() + 2);
          state.setKeySize(2);
          return true;
        }
        break;
      case LONG:
        if (targetBuffer.remaining() > 8) {
          targetBuffer.putLong((Long) key);
          state.setTotalBytes(state.getTotalBytes() + 8);
          state.setKeySize(8);
          return true;
        }
        break;
      case DOUBLE:
        if (targetBuffer.remaining() > 8) {
          targetBuffer.putDouble((Double) key);
          state.setTotalBytes(state.getTotalBytes() + 8);
          state.setKeySize(8);
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
      default:
        break;
    }
    return false;
  }

  private static boolean copyKeyBytes(ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    byte[] key = state.getKey();
    if (bytesCopied == 0 && remainingCapacity > 4) {
      targetBuffer.putInt(key.length);
      totalBytes += 4;
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
    state.setKeySize(key.length + 4);
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
}
