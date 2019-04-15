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

import edu.iu.dsc.tws.comms.api.types.ObjectType;
import edu.iu.dsc.tws.comms.api.types.PrimitiveMessageTypes;
import edu.iu.dsc.tws.comms.api.types.StringType;
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

public final class MessageTypes {

  private MessageTypes() {
  }

  public static final PrimitiveMessageTypes<Integer> INTEGER = new PrimitiveMessageTypes<>(
      true, Integer.BYTES, Integer.class, IntegerPacker.getInstance()
  );
  public static final PrimitiveMessageTypes<int[]> INTEGER_ARRAY = new PrimitiveMessageTypes<>(
      true, Integer.BYTES, int[].class, IntegerArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Character> CHAR = new PrimitiveMessageTypes<>(
      true, Character.BYTES, Character.class, CharPacker.getInstance()
  );
  public static final PrimitiveMessageTypes<char[]> CHAR_ARRAY = new PrimitiveMessageTypes<>(
      true, Character.BYTES, char[].class, CharArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Byte> BYTE = new PrimitiveMessageTypes<>(
      true, Byte.BYTES, Byte.class, BytePacker.getInstance()
  );
  public static final PrimitiveMessageTypes<byte[]> BYTE_ARRAY = new PrimitiveMessageTypes<>(
      true, Byte.BYTES, byte[].class, ByteArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Long> LONG = new PrimitiveMessageTypes<>(
      true, Long.BYTES, Long.class, LongPacker.getInstance()
  );
  public static final PrimitiveMessageTypes<long[]> LONG_ARRAY = new PrimitiveMessageTypes<>(
      true, Long.BYTES, long[].class, LongArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Double> DOUBLE = new PrimitiveMessageTypes<>(
      true, Double.BYTES, Double.class, DoublePacker.getInstance()
  );
  public static final PrimitiveMessageTypes<double[]> DOUBLE_ARRAY = new PrimitiveMessageTypes<>(
      true, Double.BYTES, double[].class, DoubleArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Float> FLOAT = new PrimitiveMessageTypes<>(
      true, Float.BYTES, Float.class, FloatPacker.getInstance()
  );
  public static final PrimitiveMessageTypes<float[]> FLOAT_ARRAY = new PrimitiveMessageTypes<>(
      true, Float.BYTES, float[].class, FloatArrayPacker.getInstance(), true
  );

  public static final PrimitiveMessageTypes<Short> SHORT = new PrimitiveMessageTypes<>(
      true, Short.BYTES, Short.class, ShortPacker.getInstance()
  );
  public static final PrimitiveMessageTypes<short[]> SHORT_ARRAY = new PrimitiveMessageTypes<>(
      true, Short.BYTES, short[].class, ShortArrayPacker.getInstance(), true
  );

  public static final StringType STRING = new StringType();

  public static final ObjectType OBJECT = new ObjectType();
}
