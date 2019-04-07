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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import static edu.iu.dsc.tws.comms.api.MessageType.BYTE;
import static edu.iu.dsc.tws.comms.api.MessageType.CHAR;
import static edu.iu.dsc.tws.comms.api.MessageType.DOUBLE;
import static edu.iu.dsc.tws.comms.api.MessageType.INTEGER;
import static edu.iu.dsc.tws.comms.api.MessageType.LONG;
import static edu.iu.dsc.tws.comms.api.MessageType.OBJECT;
import static edu.iu.dsc.tws.comms.api.MessageType.SHORT;

public final class PartialKeyDeSerializer {
  private PartialKeyDeSerializer() {
  }

  public static int keyLengthHeaderSize(InMessage message) {
    if (message.getKeyType() == MessageType.BYTE || message.getKeyType() == MessageType.OBJECT) {
      return Integer.BYTES;
    } else {
      return 0;
    }
  }

  public static Pair<Integer, Integer> createKey(InMessage message,
                                                 DataBuffer buffers, int location) {
    int keyLength = 0;
    int readBytes = 0;
    // first we need to read the key type
    MessageType keyType = message.getKeyType();
    if (INTEGER.equals(keyType)) {
      keyLength = Integer.BYTES;
    } else if (SHORT.equals(keyType)) {
      keyLength = Short.BYTES;
    } else if (LONG.equals(keyType)) {
      keyLength = Long.BYTES;
    } else if (DOUBLE.equals(keyType)) {
      keyLength = Double.BYTES;
    } else if (OBJECT.equals(keyType)) {
      keyLength = buffers.getByteBuffer().getInt(location);
      readBytes = Integer.BYTES;
    } else if (BYTE.equals(keyType)) {
      keyLength = buffers.getByteBuffer().getInt(location);
      readBytes = Integer.BYTES;
    }

    return new ImmutablePair<>(keyLength, readBytes);
  }

  public static Object createKeyObject(MessageType messageType, int length) {
    //todo replace if else with below code
//    if (messageType.getDataPacker() instanceof ArrayPacker) {
//      return ((ArrayPacker) messageType.getDataPacker())
//          .initializeEmptyArrayForByteLength(length);
//    } else {
//      return null;
//    }
    if (INTEGER.equals(messageType)) {
      return null;
    } else if (LONG.equals(messageType)) {
      return null;
    } else if (DOUBLE.equals(messageType)) {
      return null;
    } else if (SHORT.equals(messageType)) {
      return null;
    } else if (CHAR.equals(messageType)) {
      return null;
    } else if (BYTE.equals(messageType)) {
      return new byte[length];
    } else if (OBJECT.equals(messageType)) {
      return new byte[length];
    }
    return null;
  }

  public static int readFromBuffer(InMessage currentMessage, MessageType dataType,
                                   int currentLocation,
                                   DataBuffer buffer, int currentObjectLength,
                                   KryoSerializer serializer) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    if (INTEGER.equals(dataType)) {
      return deserializeInteger(currentMessage, buffer, currentLocation);
    } else if (LONG.equals(dataType)) {
      return deserializeLong(currentMessage, buffer, currentLocation);
    } else if (DOUBLE.equals(dataType)) {
      return deserializeDouble(currentMessage, buffer, currentLocation);
    } else if (SHORT.equals(dataType)) {
      return deserializeShort(currentMessage, buffer, currentLocation);
    } else if (BYTE.equals(dataType)) {
      byte[] byteVal = (byte[]) currentMessage.getDeserializingKey();
      return PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
          byteVal, startIndex, currentLocation);
    } else if (OBJECT.equals(dataType)) {
      byte[] objectVal = (byte[]) currentMessage.getDeserializingKey();
      int value = PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
          objectVal, startIndex, currentLocation);
      // at the end we switch to the actual object
      if (value == currentObjectLength) {
        Object kryoValue = serializer.deserialize(objectVal);
        currentMessage.setDeserializingKey(kryoValue);
      }
      return value;
    }
    return 0;
  }

  public static int totalBytesRead(InMessage msg, int valsRead) {
    if (INTEGER.equals(msg.getDataType())) {
      return valsRead;
    } else if (DOUBLE.equals(msg.getDataType())) {
      return valsRead;
    } else if (LONG.equals(msg.getDataType())) {
      return valsRead;
    } else if (SHORT.equals(msg.getDataType())) {
      return valsRead;
    } else if (CHAR.equals(msg.getDataType())) {
      return valsRead;
    } else if (BYTE.equals(msg.getDataType())) {
      int i5 = valsRead + msg.getUnPkCurrentBytes();
      msg.addUnPkCurrentBytes(valsRead);
      return i5;
    } else if (OBJECT.equals(msg.getDataType())) {
      int i6 = valsRead + msg.getUnPkCurrentBytes();
      msg.addUnPkCurrentBytes(valsRead);
      return i6;
    }
    return 0;
  }

  private static int deserializeInteger(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Integer.BYTES) {
      int val = byteBuffer.getInt(bufferLocation);
      message.setDeserializingKey(val);
      return Integer.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeLong(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Long.BYTES) {
      long val = byteBuffer.getLong(bufferLocation);
      message.setDeserializingKey(val);
      return Long.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeDouble(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Double.BYTES) {
      double val = byteBuffer.getDouble(bufferLocation);
      message.setDeserializingKey(val);
      return Double.BYTES;
    } else {
      return 0;
    }
  }

  private static int deserializeShort(InMessage message, DataBuffer buffers, int bufferLocation) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - bufferLocation;
    if (remaining >= Short.BYTES) {
      short val = byteBuffer.getShort(bufferLocation);
      message.setDeserializingKey(val);
      return Short.BYTES;
    } else {
      return 0;
    }
  }
}
