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
package edu.iu.dsc.tws.comms.api;

import java.lang.reflect.Array;

import edu.iu.dsc.tws.comms.api.types.TypeDefinition;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.DoubleArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.DoublePacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.LongArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.LongPacker;

public final class MessageType<T> implements TypeDefinition<T> {

  public static final MessageType<Integer> INTEGER = new MessageType<>(
      true, Integer.BYTES, Integer.class, IntegerPacker.getInstance()
  );
  public static final MessageType<int[]> INTEGER_ARRAY = new MessageType<>(
      true, Integer.BYTES, int[].class, IntegerArrayPacker.getInstance()
  );

  public static final MessageType<Character> CHAR = new MessageType<>(
      true, Character.BYTES, Character.class, null
  );
  public static final MessageType<char[]> CHAR_ARRAY = new MessageType<>(
      true, Character.BYTES, char[].class, null
  );

  public static final MessageType<Byte> BYTE = new MessageType<>(
      true, Byte.BYTES, Byte.class, null
  );
  public static final MessageType<byte[]> BYTE_ARRAY = new MessageType<>(
      true, Byte.BYTES, byte[].class, null
  );

  public static final MessageType<Long> LONG = new MessageType<>(
      true, Long.BYTES, Long.class, LongPacker.getInstance()
  );
  public static final MessageType<long[]> LONG_ARRAY = new MessageType<>(
      true, Long.BYTES, long[].class, LongArrayPacker.getInstance()
  );

  public static final MessageType<Double> DOUBLE = new MessageType<>(
      true, Double.BYTES, Double.class, DoublePacker.getInstance()
  );
  public static final MessageType<double[]> DOUBLE_ARRAY = new MessageType<>(
      true, Double.BYTES, double[].class, DoubleArrayPacker.getInstance()
  );

  public static final MessageType<Short> SHORT = new MessageType<>(
      true, Short.BYTES, Short.class, null
  );
  public static final MessageType<short[]> SHORT_ARRAY = new MessageType<>(
      true, Short.BYTES, short[].class, null
  );

  public static final MessageType<String> STRING = new MessageType<>(
      true, Character.BYTES, String.class, null
  );

  public static final MessageType<Object> OBJECT = new MessageType<>(
      true, 0, Object.class, null
  );

  public static final MessageType<Object> EMPTY = new MessageType<>(
      true, 0, Object.class, null
  );

  public static final MessageType<Object> CUSTOM = new MessageType<>(
      true, 0, Object.class, null
  );

  private boolean isPrimitive;
  private int size;
  private Class<T> clazz;
  private DataPacker<T> dataPacker;

  //todo remove
  private KeyPacker keyPacker;

  private MessageType(boolean primitive, int size, Class<T> clazz, DataPacker<T> dataPacker) {
    this.isPrimitive = primitive;
    this.size = size;
    this.clazz = clazz;
    this.dataPacker = dataPacker;
  }

  /**
   * Checks if the given message type is of a primitive type
   * if the type is primitive then we do not need to add data length to the data buffers
   */
  public boolean isPrimitive() {
    return isPrimitive;
  }

  @Override
  public int getUnitSizeInBytes() {
    return this.size;
  }

  @Override
  public int getDataSizeInBytes(T data) {
    if (!this.isArray()) {
      return this.getUnitSizeInBytes();
    } else {
      return Array.getLength(data) * this.getUnitSizeInBytes();
    }
  }

  /**
   * checks if the type is a multi message, not to be confused with the aggregated multi-messages
   * that are passed through the network when optimized communications such as reduce are performed
   * this refers to the original type of the message
   *
   * @deprecated This will be removed in future releases
   */
  @Deprecated
  public boolean isMultiMessageType() {
    return false;
  }

  /**
   * Specify a custom data packer
   *
   * @return a custom data packer
   */
  public DataPacker<T> getDataPacker() {
    return dataPacker;
  }

  @Override
  public boolean isArray() {
    return false;
  }

  /**
   * Set the custom data packer
   *
   * @param customPacker set the custom packer
   */
  public void setCustomPacker(DataPacker customPacker) {
    this.dataPacker = customPacker;
  }

  /**
   * Get the key packer associated with this type
   *
   * @return the key packer
   */
  public KeyPacker getKeyPacker() {
    return keyPacker;
  }

  /**
   * Set the key packer associated with this type
   *
   * @param keyPacker key packer
   */
  public void setKeyPacker(KeyPacker keyPacker) {
    this.keyPacker = keyPacker;
  }

  /**
   * Size of an unit
   *
   * @return the size of an unit
   * @deprecated use {@link #getUnitSizeInBytes()} instead
   */
  @Deprecated
  public int getSize() {
    return size;
  }

  public Class<T> getClazz() {
    return clazz;
  }
}
