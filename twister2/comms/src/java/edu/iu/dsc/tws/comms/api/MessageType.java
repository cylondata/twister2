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
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.ByteArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.BytePacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.CharArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.CharPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.DoubleArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.DoublePacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.FloatArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.FloatPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.IntegerPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.LongArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.LongPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.ShortArrayPacker;
import edu.iu.dsc.tws.comms.dfw.io.types.primitive.ShortPacker;

public final class MessageType<T> implements TypeDefinition<T> {

  public static final MessageType<Integer> INTEGER = new MessageType<>(
      true, Integer.BYTES, Integer.class, IntegerPacker.getInstance()
  );
  public static final MessageType<int[]> INTEGER_ARRAY = new MessageType<>(
      true, Integer.BYTES, int[].class, IntegerArrayPacker.getInstance(), true
  );

  public static final MessageType<Character> CHAR = new MessageType<>(
      true, Character.BYTES, Character.class, CharPacker.getInstance()
  );
  public static final MessageType<char[]> CHAR_ARRAY = new MessageType<>(
      true, Character.BYTES, char[].class, CharArrayPacker.getInstance(), true
  );

  public static final MessageType<Byte> BYTE = new MessageType<>(
      true, Byte.BYTES, Byte.class, BytePacker.getInstance()
  );
  public static final MessageType<byte[]> BYTE_ARRAY = new MessageType<>(
      true, Byte.BYTES, byte[].class, ByteArrayPacker.getInstance(), true
  );

  public static final MessageType<Long> LONG = new MessageType<>(
      true, Long.BYTES, Long.class, LongPacker.getInstance()
  );
  public static final MessageType<long[]> LONG_ARRAY = new MessageType<>(
      true, Long.BYTES, long[].class, LongArrayPacker.getInstance(), true
  );

  public static final MessageType<Double> DOUBLE = new MessageType<>(
      true, Double.BYTES, Double.class, DoublePacker.getInstance()
  );
  public static final MessageType<double[]> DOUBLE_ARRAY = new MessageType<>(
      true, Double.BYTES, double[].class, DoubleArrayPacker.getInstance(), true
  );

  public static final MessageType<Float> FLOAT = new MessageType<>(
      true, Float.BYTES, Float.class, FloatPacker.getInstance()
  );
  public static final MessageType<float[]> FLOAT_ARRAY = new MessageType<>(
      true, Float.BYTES, float[].class, FloatArrayPacker.getInstance(), true
  );

  public static final MessageType<Short> SHORT = new MessageType<>(
      true, Short.BYTES, Short.class, ShortPacker.getInstance()
  );
  public static final MessageType<short[]> SHORT_ARRAY = new MessageType<>(
      true, Short.BYTES, short[].class, ShortArrayPacker.getInstance(), true
  );

  public static final MessageType<String> STRING = new MessageType<>(
      false, Character.BYTES, String.class, null
  );

  public static final MessageType<Object> OBJECT = new MessageType<>(
      false, 0, Object.class, null
  );

  public static final MessageType<Object> CUSTOM = new MessageType<>(
      false, 0, Object.class, null
  );

  private boolean isPrimitive;
  private int size;
  private Class<T> clazz;
  private DataPacker<T> dataPacker;
  private boolean isArray = false;

  //todo remove
  private KeyPacker keyPacker;

  private MessageType(boolean primitive, int size, Class<T> clazz, DataPacker<T> dataPacker) {
    this.isPrimitive = primitive;
    this.size = size;
    this.clazz = clazz;
    this.dataPacker = dataPacker;
  }

  private MessageType(boolean primitive, int size, Class<T> clazz,
                      DataPacker<T> dataPacker, boolean isArray) {
    this.isPrimitive = primitive;
    this.size = size;
    this.clazz = clazz;
    this.dataPacker = dataPacker;
    this.isArray = isArray;
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
    return this.isArray;
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
