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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class KeyDeserializer {
  private static final Logger LOG = Logger.getLogger(KeyDeserializer.class.getName());

  private KeyDeserializer() {
  }

  /**
   * Deserializers the key that is contained in the buffer list passed to the method
   *
   * @param keyType the type of the key to be retrieved
   * @param buffers the buffers that belong to the message, the key is contained in these buffers
   * @param deserializer the deserializer to be used if the object needs to be deserialized
   * @return a pair of key length and the key
   */
  public static Pair<Integer, Object> deserializeKey(MessageType keyType,
                                                     List<DataBuffer> buffers,
                                                     KryoSerializer deserializer) {
    int currentIndex = 0;
    int keyLength = 0;
    Object key = null;
    //Used when there are multiple keys
    int keyCount;
    // first we need to read the key type
    switch (keyType) {
      case INTEGER:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        ByteBuffer byteBuffer = buffers.get(currentIndex).getByteBuffer();
//        LOG.info(String.format("Key deserialize position %d", byteBuffer.position()));
        key = byteBuffer.getInt();
        keyLength = 4;
        break;
      case SHORT:
        currentIndex = getReadIndex(buffers, currentIndex, Short.BYTES);
        key = buffers.get(currentIndex).getByteBuffer().getShort();
        keyLength = 2;
        break;
      case LONG:
        currentIndex = getReadIndex(buffers, currentIndex, Long.BYTES);
        key = buffers.get(currentIndex).getByteBuffer().getLong();
        keyLength = 8;
        break;
      case DOUBLE:
        currentIndex = getReadIndex(buffers, currentIndex, Double.BYTES);
        key = buffers.get(currentIndex).getByteBuffer().getDouble();
        keyLength = 8;
        break;
      case OBJECT:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = DataDeserializer.deserializeObject(buffers, keyLength, deserializer);
        break;
      case BYTE:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = readBytes(buffers, keyLength);
        break;
      case STRING:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = new String(readBytes(buffers, keyLength));
        break;
      case MULTI_FIXED_BYTE:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES * 2);
        keyCount = buffers.get(currentIndex).getByteBuffer().getInt();
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = readMultiBytes(buffers, keyLength, keyCount);
        break;
      default:
        break;
    }
    return new ImmutablePair<>(keyLength, key);
  }

  /**
   * reads the next key of given type in the MPIBuffers and returns it as a byte[]
   *
   * @param keyType type of the key
   * @param buffers buffers that contain the data
   * @return key as ByteBuffer
   */
  public static Pair<Integer, Object> getKeyAsByteArray(MessageType keyType,
                                                        List<DataBuffer> buffers) {
    int currentIndex = 0;
    //Used when there are multiple keys
    int keyCount;
    byte[] tempArray = null;
    Object key = null;
    int keyLength = 0;
    switch (keyType) {
      case INTEGER:
        tempArray = new byte[Integer.BYTES];
        keyLength = Integer.BYTES;
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        buffers.get(currentIndex).getByteBuffer().get(tempArray);
        break;
      case SHORT:
        tempArray = new byte[Short.BYTES];
        keyLength = Short.BYTES;
        currentIndex = getReadIndex(buffers, currentIndex, Short.BYTES);
        buffers.get(currentIndex).getByteBuffer().get(tempArray);
        break;
      case LONG:
        tempArray = new byte[Long.BYTES];
        keyLength = Long.BYTES;
        currentIndex = getReadIndex(buffers, currentIndex, Long.BYTES);
        buffers.get(currentIndex).getByteBuffer().get(tempArray);
        break;
      case DOUBLE:
        tempArray = new byte[Double.BYTES];
        keyLength = Double.BYTES;
        currentIndex = getReadIndex(buffers, currentIndex, Double.BYTES);
        buffers.get(currentIndex).getByteBuffer().get(tempArray);
        break;
      case OBJECT:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        tempArray = readBytes(buffers, keyLength);
        break;
      case BYTE:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        tempArray = readBytes(buffers, keyLength);
        break;
      case STRING:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        tempArray = readBytes(buffers, keyLength);
        break;
      case MULTI_FIXED_BYTE:
        currentIndex = getReadIndex(buffers, currentIndex, Integer.BYTES * 2);
        keyCount = buffers.get(currentIndex).getByteBuffer().getInt();
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = readMultiBytes(buffers, keyLength, keyCount);
        return new ImmutablePair<>(keyLength, key);
      default:
        tempArray = new byte[0];
        break;
    }
    return new ImmutablePair<>(keyLength, tempArray);
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

  private static int getReadIndex(List<DataBuffer> buffers, int currentIndex, int expectedSize) {
    for (int i = currentIndex; i < buffers.size(); i++) {
      ByteBuffer byteBuffer = buffers.get(i).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining > expectedSize) {
        return i;
      }
    }
    throw new RuntimeException("Something is wrong in the buffer management");
  }

  private static Object readMultiBytes(List<DataBuffer> buffers, int keyLength, int keyCount) {
    List<byte[]> keys = new ArrayList<>();
    int singleKeyLength = keyLength / keyCount;
    for (int i = 0; i < keyCount; i++) {
      keys.add(readBytes(buffers, singleKeyLength));
    }
    return keys;
  }
}
