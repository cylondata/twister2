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
package edu.iu.dsc.tws.data.utils;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.data.memory.utils.DataMessageType;

/**
 * Utils class used to Deserialize data from the memory manager
 */
public final class MemoryDeserializer {

  private MemoryDeserializer() {
  }

  public static Object deserializeKey(ByteBuffer key, DataMessageType keyType,
                                      KryoMemorySerializer serializer) {
    switch (keyType) {
      case INTEGER:
        return key.getInt();
      case DOUBLE:
        return key.getDouble();
      case SHORT:
        return key.getShort();
      case OBJECT:
        return serializer.deserialize(key.array());
      default:
        break;
    }
    return null;
  }

  public static Object deserializeValue(ByteBuffer value, DataMessageType valueType,
                                        KryoMemorySerializer serializer) {
    switch (valueType) {
      case INTEGER:
        return deserializeInteger(value);
      case DOUBLE:
        return deserializeDouble(value);
      case SHORT:
        return deserializeShort(value);
      case OBJECT:
        return deserializeObject(value, serializer);
      default:
        break;
    }
    return null;
  }

  public static int[] deserializeInteger(ByteBuffer data) {
    int canRead = data.remaining();
    if (canRead % 4 != 0) {
      throw new RuntimeException("Integer data buffer cannot be divided to integers,"
          + " number of bytes does not dived by 4");
    }
    int[] result = new int[canRead / 4];
    for (int i = 0; i < result.length; i++) {
      result[i] = data.getInt();
    }
    return result;
  }

  public static double[] deserializeDouble(ByteBuffer data) {
    int canRead = data.remaining();
    if (canRead % 8 != 0) {
      throw new RuntimeException("Double data buffer cannot be divided to doubles,"
          + " number of bytes does not dived by 8");
    }
    double[] result = new double[canRead / 8];
    for (int i = 0; i < result.length; i++) {
      result[i] = data.getDouble();
    }
    return result;
  }

  public static short[] deserializeShort(ByteBuffer data) {
    int canRead = data.remaining();
    if (canRead % 2 != 0) {
      throw new RuntimeException("Short data buffer cannot be divided to shorts,"
          + " number of bytes does not dived by 2");
    }
    short[] result = new short[canRead / 2];
    for (int i = 0; i < result.length; i++) {
      result[i] = data.getShort();
    }
    return result;
  }

  public static Object deserializeObject(ByteBuffer data, KryoMemorySerializer serializer) {
    int length = data.getInt();
    if (length != data.remaining()) {
      throw new RuntimeException("The given data buffer does not have the bytes for the object");
    }
    //TODO: check if ByteBuffer.array only returns the remining data bytes
    return serializer.deserialize(data.array());
  }
}
